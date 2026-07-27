"""A lightweight TransE knowledge-graph embedding model.

TransE (Bordes et al., NeurIPS 2013, "Translating Embeddings for Modeling
Multi-relational Data") models each triple ``(h, r, t)`` so that the embedding
of the head plus the relation is close to the tail: ``h + r ~= t``. It is the
canonical lightweight baseline — one vector per entity and per relation, trained
with a margin-ranking loss against corrupted (negatively sampled) triples.

The implementation is intentionally compact: full-batch shuffling with uniform
negative sampling, Adam, and the standard unit-norm constraint on entity
embeddings. It runs on MPS (Apple Silicon), CUDA, or CPU.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import torch
from torch import nn
from torch.nn import functional as F  # noqa: N812

logger = logging.getLogger("cli")

# Probability of corrupting the head (vs. the tail) when sampling a negative.
_HEAD_CORRUPTION_PROB = 0.5

# Loss modes. ``adversarial`` is self-adversarial negative sampling (Sun et al.,
# RotatE, ICLR 2019): a logistic loss whose multiple negatives are softmax-
# weighted by their own scores, so hard negatives dominate — the standard fix
# for the gradient starvation that uniform random negatives cause on large
# graphs. ``max_margin`` is hardest-of-N margin ranking; ``margin`` is the
# classic mean-over-negatives margin ranking (Bordes et al., 2013).
LOSS_MODES = ("adversarial", "max_margin", "margin")


def resolve_device(device: str = "auto") -> torch.device:
    """Resolve a device string, preferring MPS/CUDA when ``"auto"``."""
    if device != "auto":
        return torch.device(device)
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


# Clear the device allocator cache this often (in batches). On MPS, the caching
# allocator fragments over a long run with large (B, N, d) negative tensors,
# slowing each successive epoch by several-fold; periodic clearing keeps the
# step time flat.
_CACHE_CLEAR_EVERY = 200


def _empty_device_cache(device: torch.device) -> None:
    """Release cached allocator memory to combat MPS/CUDA fragmentation."""
    if device.type == "mps":
        torch.mps.empty_cache()
    elif device.type == "cuda":
        torch.cuda.empty_cache()


@dataclass
class TransEConfig:
    """Hyperparameters for TransE training."""

    dim: int = 128
    epochs: int = 10
    batch_size: int = 8192
    lr: float = 0.01
    margin: float = 1.0
    p_norm: int = 1
    negatives: int = 1
    loss: str = "margin"
    adv_temperature: float = 1.0
    seed: int = 42
    device: str = "auto"


class TransE(nn.Module):
    """TransE scoring model with selectable negative-sampling loss.

    Args:
        n_entities: Number of distinct entities.
        n_relations: Number of distinct relations.
        dim: Embedding dimensionality.
        p_norm: Norm used for the dissimilarity (1 = L1, 2 = L2).
        margin: Margin / gamma for the ranking and logistic losses.
        loss: One of :data:`LOSS_MODES`.
        adv_temperature: Softmax temperature for self-adversarial weighting.
    """

    def __init__(  # noqa: PLR0913
        self,
        n_entities: int,
        n_relations: int,
        dim: int = 128,
        p_norm: int = 1,
        margin: float = 1.0,
        loss: str = "margin",
        adv_temperature: float = 1.0,
    ) -> None:
        super().__init__()
        self.n_entities = n_entities
        self.p_norm = p_norm
        self.margin = margin
        self.loss_mode = loss
        self.adv_temperature = adv_temperature
        self.entity = nn.Embedding(n_entities, dim)
        self.relation = nn.Embedding(n_relations, dim)

        # Standard Bordes et al. initialisation: uniform(-6/sqrt(d), 6/sqrt(d)),
        # with relation vectors L2-normalised once at init.
        bound = 6.0 / (dim**0.5)
        nn.init.uniform_(self.entity.weight, -bound, bound)
        nn.init.uniform_(self.relation.weight, -bound, bound)
        with torch.no_grad():
            self.relation.weight.div_(
                self.relation.weight.norm(p=2, dim=1, keepdim=True).clamp_min(1e-12)
            )

    def _pos_distance(
        self, h: torch.Tensor, r: torch.Tensor, t: torch.Tensor
    ) -> torch.Tensor:
        """Dissimilarity ``||h + r - t||_p`` for a batch of triples (shape ``(B,)``)."""
        return torch.norm(
            self.entity(h) + self.relation(r) - self.entity(t), p=self.p_norm, dim=1
        )

    def _neg_distance(
        self,
        h: torch.Tensor,
        r: torch.Tensor,
        t: torch.Tensor,
        neg_ent: torch.Tensor,
        corrupt_head: torch.Tensor,
    ) -> torch.Tensor:
        """Distances for ``(B, N)`` corrupted triples sharing each positive's relation.

        ``neg_ent`` are the replacement entities; ``corrupt_head`` selects, per
        positive, whether they replace the head (``||neg + r - t||``) or the tail
        (``||h + r - neg||``).
        """
        he, re, te = self.entity(h), self.relation(r), self.entity(t)  # (B, d)
        ne = self.entity(neg_ent)  # (B, N, d)
        base = torch.where(
            corrupt_head.view(-1, 1, 1),
            (re - te).unsqueeze(1),  # head corrupted: neg + (r - t)
            (he + re).unsqueeze(1),  # tail corrupted: (h + r) - neg
        )
        sign = (corrupt_head.float() * 2 - 1).view(-1, 1, 1)  # +1 head, -1 tail
        return torch.norm(base + sign * ne, p=self.p_norm, dim=2)  # (B, N)

    def loss(
        self,
        pos: tuple[torch.Tensor, torch.Tensor, torch.Tensor],
        neg_ent: torch.Tensor,
        corrupt_head: torch.Tensor,
    ) -> torch.Tensor:
        """Training loss for a batch of positives against ``(B, N)`` negatives."""
        h, r, t = pos
        pos_d = self._pos_distance(h, r, t)  # (B,)
        neg_d = self._neg_distance(h, r, t, neg_ent, corrupt_head)  # (B, N)

        if self.loss_mode == "adversarial":
            # Self-adversarial: weight each negative by softmax of its score, so
            # hard (close) negatives dominate; weights are detached (no grad).
            weights = torch.softmax(
                self.adv_temperature * (self.margin - neg_d), dim=1
            ).detach()
            pos_loss = -F.logsigmoid(self.margin - pos_d)  # (B,)
            neg_loss = -(weights * F.logsigmoid(neg_d - self.margin)).sum(dim=1)
            return (pos_loss + neg_loss).mean()
        if self.loss_mode == "max_margin":
            # Hardest negative per positive (smallest distance).
            hardest = neg_d.min(dim=1).values
            return torch.clamp(self.margin + pos_d - hardest, min=0).mean()
        # Classic margin ranking, averaged over the sampled negatives.
        return torch.clamp(self.margin + pos_d.unsqueeze(1) - neg_d, min=0).mean()

    @torch.no_grad()
    def normalize_entities(self) -> None:
        """Project entity embeddings back onto the unit sphere (TransE constraint)."""
        self.entity.weight.div_(
            self.entity.weight.norm(p=2, dim=1, keepdim=True).clamp_min(1e-12)
        )


def _sample_negatives(
    batch: int,
    n_neg: int,
    n_entities: int,
    generator: torch.Generator,
    device: torch.device,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Sample ``(B, N)`` replacement entities and a ``(B,)`` head/tail-corrupt mask.

    Random tensors are drawn on a CPU generator and moved to ``device`` — MPS
    does not reliably support device-side generators.
    """
    neg_ent = torch.randint(n_entities, (batch, n_neg), generator=generator).to(device)
    corrupt_head = (torch.rand(batch, generator=generator) < _HEAD_CORRUPTION_PROB).to(
        device
    )
    return neg_ent, corrupt_head


def train_transe(  # noqa: PLR0913
    head: np.ndarray,
    relation: np.ndarray,
    tail: np.ndarray,
    n_entities: int,
    n_relations: int,
    config: TransEConfig,
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Train TransE and return entity/relation embeddings plus a training log.

    Args:
        head: ``(n_triples,)`` head indices.
        relation: ``(n_triples,)`` relation indices.
        tail: ``(n_triples,)`` tail indices.
        n_entities: Number of entities.
        n_relations: Number of relations.
        config: Training hyperparameters.

    Returns:
        Tuple of ``(entity_emb, relation_emb, log)`` where the embeddings are
        float32 numpy arrays of shape ``(n_entities, dim)`` / ``(n_relations,
        dim)`` and ``log`` holds per-epoch losses and run metadata.
    """
    if config.loss not in LOSS_MODES:
        msg = f"loss must be one of {LOSS_MODES}, got {config.loss!r}"
        raise ValueError(msg)

    torch.manual_seed(config.seed)
    device = resolve_device(config.device)
    logger.info(
        "Training TransE on %s (dim=%d, epochs=%d, loss=%s, negatives=%d)",
        device,
        config.dim,
        config.epochs,
        config.loss,
        config.negatives,
    )

    model = TransE(
        n_entities=n_entities,
        n_relations=n_relations,
        dim=config.dim,
        p_norm=config.p_norm,
        margin=config.margin,
        loss=config.loss,
        adv_temperature=config.adv_temperature,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=config.lr)

    h = torch.from_numpy(head.astype(np.int64)).to(device)
    r = torch.from_numpy(relation.astype(np.int64)).to(device)
    t = torch.from_numpy(tail.astype(np.int64)).to(device)
    n_triples = h.shape[0]

    # CPU generators (moved to device as needed) so shuffling and corruption
    # are reproducible across CPU/CUDA/MPS, which lack uniform generator support.
    shuffle_gen = torch.Generator().manual_seed(config.seed)
    corrupt_gen = torch.Generator().manual_seed(config.seed + 1)

    epoch_losses: list[float] = []
    for epoch in range(config.epochs):
        model.normalize_entities()
        perm = torch.randperm(n_triples, generator=shuffle_gen)
        total, n_batches = 0.0, 0

        for step, start in enumerate(range(0, n_triples, config.batch_size)):
            idx = perm[start : start + config.batch_size].to(device)
            ph, pr, pt = h[idx], r[idx], t[idx]

            neg_ent, corrupt_head = _sample_negatives(
                ph.shape[0], config.negatives, n_entities, corrupt_gen, device
            )
            batch_loss = model.loss((ph, pr, pt), neg_ent, corrupt_head)

            optimizer.zero_grad()
            batch_loss.backward()
            optimizer.step()

            total += float(batch_loss.detach())
            n_batches += 1
            if step % _CACHE_CLEAR_EVERY == _CACHE_CLEAR_EVERY - 1:
                _empty_device_cache(device)

        _empty_device_cache(device)
        mean_loss = total / max(n_batches, 1)
        epoch_losses.append(mean_loss)
        logger.info("  epoch %d/%d  loss=%.4f", epoch + 1, config.epochs, mean_loss)

    model.normalize_entities()
    entity_emb = model.entity.weight.detach().cpu().numpy().astype(np.float32)
    relation_emb = model.relation.weight.detach().cpu().numpy().astype(np.float32)

    log = {
        "device": str(device),
        "n_entities": n_entities,
        "n_relations": n_relations,
        "n_triples": int(n_triples),
        "epoch_losses": epoch_losses,
        "final_loss": epoch_losses[-1] if epoch_losses else None,
        "config": vars(config),
    }
    return entity_emb, relation_emb, log

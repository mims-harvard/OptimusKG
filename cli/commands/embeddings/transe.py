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

logger = logging.getLogger("cli")


def resolve_device(device: str = "auto") -> torch.device:
    """Resolve a device string, preferring MPS/CUDA when ``"auto"``."""
    if device != "auto":
        return torch.device(device)
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


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
    seed: int = 42
    device: str = "auto"


class TransE(nn.Module):
    """TransE scoring model with margin-ranking loss.

    Args:
        n_entities: Number of distinct entities.
        n_relations: Number of distinct relations.
        dim: Embedding dimensionality.
        p_norm: Norm used for the dissimilarity (1 = L1, 2 = L2).
        margin: Margin gamma in the ranking loss.
    """

    def __init__(
        self,
        n_entities: int,
        n_relations: int,
        dim: int = 128,
        p_norm: int = 1,
        margin: float = 1.0,
    ) -> None:
        super().__init__()
        self.n_entities = n_entities
        self.p_norm = p_norm
        self.margin = margin
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

    def _distance(
        self, h: torch.Tensor, r: torch.Tensor, t: torch.Tensor
    ) -> torch.Tensor:
        """Dissimilarity ``||h + r - t||_p`` for a batch of index tensors."""
        return torch.norm(
            self.entity(h) + self.relation(r) - self.entity(t),
            p=self.p_norm,
            dim=1,
        )

    def forward(
        self,
        pos: tuple[torch.Tensor, torch.Tensor, torch.Tensor],
        neg: tuple[torch.Tensor, torch.Tensor, torch.Tensor],
    ) -> torch.Tensor:
        """Margin-ranking loss for a batch of positive and negative triples."""
        pos_d = self._distance(*pos)
        neg_d = self._distance(*neg)
        # Positives should score lower (smaller distance) than negatives.
        return torch.clamp(self.margin + pos_d - neg_d, min=0).mean()

    @torch.no_grad()
    def normalize_entities(self) -> None:
        """Project entity embeddings back onto the unit sphere (TransE constraint)."""
        self.entity.weight.div_(
            self.entity.weight.norm(p=2, dim=1, keepdim=True).clamp_min(1e-12)
        )


def _corrupt(
    head: torch.Tensor,
    tail: torch.Tensor,
    n_entities: int,
    generator: torch.Generator,
    device: torch.device,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Corrupt either the head or the tail of each triple (uniformly).

    Returns the corrupted ``(head, tail)`` tensors; the relation is unchanged.
    """
    batch = head.shape[0]
    rand_entities = torch.randint(
        n_entities, (batch,), generator=generator, device=device
    )
    corrupt_head = torch.rand(batch, generator=generator, device=device) < 0.5
    neg_head = torch.where(corrupt_head, rand_entities, head)
    neg_tail = torch.where(corrupt_head, tail, rand_entities)
    return neg_head, neg_tail


def train_transe(
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
    torch.manual_seed(config.seed)
    device = resolve_device(config.device)
    logger.info("Training TransE on %s (dim=%d, epochs=%d)", device, config.dim, config.epochs)

    model = TransE(
        n_entities=n_entities,
        n_relations=n_relations,
        dim=config.dim,
        p_norm=config.p_norm,
        margin=config.margin,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=config.lr)

    h = torch.from_numpy(head.astype(np.int64)).to(device)
    r = torch.from_numpy(relation.astype(np.int64)).to(device)
    t = torch.from_numpy(tail.astype(np.int64)).to(device)
    n_triples = h.shape[0]

    # Separate generators so shuffling and corruption are reproducible and
    # independent of device default RNG state.
    shuffle_gen = torch.Generator(device=device).manual_seed(config.seed)
    corrupt_gen = torch.Generator(device=device).manual_seed(config.seed + 1)

    epoch_losses: list[float] = []
    for epoch in range(config.epochs):
        model.normalize_entities()
        perm = torch.randperm(n_triples, generator=shuffle_gen, device=device)
        total, n_batches = 0.0, 0

        for start in range(0, n_triples, config.batch_size):
            idx = perm[start : start + config.batch_size]
            ph, pr, pt = h[idx], r[idx], t[idx]

            batch_loss = torch.zeros((), device=device)
            for _ in range(config.negatives):
                nh, nt = _corrupt(ph, pt, n_entities, corrupt_gen, device)
                batch_loss = batch_loss + model((ph, pr, pt), (nh, pr, nt))
            batch_loss = batch_loss / config.negatives

            optimizer.zero_grad()
            batch_loss.backward()
            optimizer.step()

            total += float(batch_loss.detach())
            n_batches += 1

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

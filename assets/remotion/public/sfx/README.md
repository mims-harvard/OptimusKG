# Sound effects

Drop short mp3 files here matching the names registered in
`src/sounds/index.ts`. Recommended starter pack:

| Filename | Used for | Source ideas |
|---|---|---|
| `pop-soft.mp3` | chip / node "appear" | mixkit "soft pop" |
| `whoosh-short.mp3` | slide-in motions | mixkit "ui woosh" |
| `tick-light.mp3` | counter ticks, sidebar cycle | freesound "ui tick" |
| `chime-soft.mp3` | landings, scene resolution | mixkit "completion chime" |
| `swish.mp3` | letter-spacing collapse / hero reveals | mixkit "fast woosh" |
| `type-key.mp3` | typewriter key | freesound "mechanical key" |

Keep volumes mixed quietly in the source files; the registry's `volume`
defaults are conservative (0.25–0.45). Easier to bump up at the call site
than to discover something clipped after rendering.

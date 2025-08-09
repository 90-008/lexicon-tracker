a webapp and server that monitors the jetstream and tracks the different
lexicons as they are created or deleted. it shows you which collections are most
active on the network.

for backend it uses rust with fjall as db, the frontend is built with sveltekit.

see [here](https://gaze.systems/nsid-tracker) for a hosted instance of it.

## performance / storage

it uses about 50MB of space for 620M recorded events (events being just
timestamp in seconds and deleted boolean for now). and around 50-60ms for
querying 300-400k events.

this is on a machine with AMD EPYC 7281 (32) @ 2.100GHz.

## running

### with nix

- build the server: `nix build git+https://tangled.sh/@poor.dog/nsid-tracker#server`
- build the client: `nix build git+https://tangled.sh/@poor.dog/nsid-tracker#client`

### manually

you'll need rust and bun.

then:

```bash
# start the backend
cd server && cargo run

# in another terminal, start the frontend
cd client && bun install && bun run -b dev
```

the frontend will be available at `http://localhost:5173` and the backend at `http://localhost:3713`.

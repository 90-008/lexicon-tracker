a webapp and server that monitors bluesky's jetsream and counts how many times different types of records are created or deleted. it shows you which collections (like posts, likes, follows, etc.) are most active on the network.

for backend it uses rust with fjall as db, the frontend is built with sveltekit.

see [here](https://gaze.systems/nsid-tracker) for a hosted instance of it.

## running

### with nix

- run the server: `nix run git+https://tangled.sh/@poor.dog/nsid-tracker#server`
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

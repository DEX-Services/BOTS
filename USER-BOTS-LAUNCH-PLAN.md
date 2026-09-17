# Plan: Ship "Any User Can Spin Up a Bot" for Real

Status: planning only, nothing implemented from this file yet.

## What already exists (don't rebuild this)

Before planning new work, it's worth being clear that the `bots` service is
not a stub — most of what "a user can create and run a real bot" needs is
already built:

- **Real backend, not mocked**: `internal/api/api.go` has full CRUD —
  `POST /bots` (create), `GET /bots` (list mine), `POST /bots/{id}/start`,
  `POST /bots/{id}/stop`, `DELETE /bots/{id}`, `POST /bots/{id}/copy`
  (clone from marketplace), all behind `requireAuth` and scoped to
  `claims.UserID` (see `handleGet`'s `bot.UserID != claims.UserID` check).
- **A real strategy engine**: `internal/strategy/strategy.go` implements
  actual accounting (avg-cost inventory, equity curve, drawdown, trade
  history) and multiple live strategies — `spot_grid`, `futures_grid`,
  `spot_dca`, `futures_dca`, `futures_twap`, `market_maker` are all marked
  `Available: true` today.
- **A real execution runtime**: `internal/runtime/runtime.go`'s `Manager`
  runs each started bot as its own worker goroutine, ticking on an
  interval, calling into `internal/engine/client.go`'s `SubmitOrder` /
  `CancelOrder` / `Balance` — i.e. bots place real orders against the real
  matching engine under the user's own account, not a simulation.
- **The frontend (`TradingBots.tsx`) is already wired to this real
  backend** via `botsApi.ts` — create, start, stop, delete, copy, my-bots
  polling, and the public marketplace all call the real endpoints above.
  This is the part that was *not* frontend-only; only the AI Agent
  create-flow was (now disabled).

So this plan is about **hardening, verifying, and productizing** an
already-functional system for real users with real money — not building
bot execution from zero.

## What "properly working for any user" actually requires

### 1. Verify each `Available: true` strategy end-to-end, live

For each of `spot_grid`, `futures_grid`, `spot_dca`, `futures_dca`,
`futures_twap`, `market_maker`:
- Create a bot as a real (non-admin) test user via the actual UI.
- Start it, confirm real orders land in the matching engine
  (`internal/engine/client.go`'s `SubmitOrder`), fill, and that
  `computeStats` in `runtime.go` reflects real fills (net PnL, ROI,
  matched trades, drawdown) — not just that the bot "runs" without
  crashing.
- Stop it, confirm open orders are cancelled and funds aren't left
  stranded in a lock.
- Delete it, confirm no orphaned resting orders remain on the book.

This is the highest-value, lowest-effort step: if the engine/strategy
logic is already solid (likely, given its depth), this mostly just needs
confirming rather than writing new code.

### 2. Capital safety: what happens when a bot's funds run out or the market moves against it

- Confirm there's a hard cap tying a bot's order sizing to its declared
  `investment`, so a strategy bug can't place orders beyond what the user
  actually funded.
- Confirm `AvailableBalance`/lock checks in `internal/backend/client.go`
  are consulted *before* `SubmitOrder`, not just relied on to fail at the
  engine — a rejected order from insufficient balance should stop/pause
  the bot with a visible `error` on the bot record (the `Bot.error` field
  already exists and is already rendered in `MyBotCard`), not silently
  retry forever.
- Decide and enforce a minimum investment per strategy (grid strategies in
  particular can misbehave with too little capital to place a meaningful
  ladder).

### 3. Crash/restart safety for running bots

- Confirm what happens to a running bot's worker if the `bots` process
  itself restarts (deploy, crash) — does `Manager` resume every bot that
  was `status: running` from the store on boot, or does it silently stop
  ticking until the user notices and manually restarts it? A user's bot
  going silently idle after a deploy is a real trust problem.
- Confirm a worker that errors out (`errorIsRetryable` exists —
  check what it actually covers) degrades to a paused/error state with a
  visible message, rather than crash-looping or silently dying.

### 4. Rate limits and abuse prevention

- Now that any user can create bots, decide a per-user cap on number of
  concurrently running bots (prevents one user from spinning up hundreds
  and overwhelming the matching engine or the `bots` service's own
  goroutine count).
- Confirm `engine.Client`'s `acquire`/`release` semaphore (already present)
  is tuned for real concurrent user load, not just the market-maker's
  internal usage it may have been sized for originally.

### 5. Fee handling

- Confirm bot-placed orders pay the same maker/taker fees as manually
  placed orders (no accidental fee bypass for automated trading), and that
  fee revenue from bot trades is attributed/reported the same way it is
  for the prediction-service fee category (`internal/backendclient`'s
  `SettleFee` pattern in prediction-service is a useful reference for how
  this should look for bots too, if it doesn't already exist here).

### 6. Marketplace/copy semantics

- `copyBot` clones a public bot's config into a new bot under the copying
  user's account (`handleCopy`) — confirm this copies config only, never
  the original owner's funds, keys, or running state, and that a copied
  bot starts in a safe `draft`/stopped state requiring the new owner to
  explicitly fund and start it.
- Decide whether a bot must have a minimum real trading history before
  it's eligible to appear in the public marketplace (prevents a
  freshly-created, unproven bot from looking like a track record).

### 7. Frontend polish once the above is verified

- `TradingBots.tsx` already polls `getMyBots` every 5s while authed — fine
  for now; revisit only if step 4's cap makes many-bots-per-user common
  enough that this becomes a real load concern.
- Once AI Agent is ready to be re-enabled (separate, future effort — not
  part of this plan), restore the commented-out import/route in
  `Dex New Frontend/src/App.tsx` and the two disabled buttons in
  `TradingBots.tsx` and `MarketHeader.tsx`, wiring the flow's final
  "accept" step to `botsApi.createBot` instead of just local state.

## Suggested order

1. Step 1 (live verification) first — it's the cheapest way to find out
   how much of steps 2-6 are already handled correctly vs. actually
   missing, since a lot of this may already be covered by existing code
   that just hasn't been exercised under a real non-admin user account.
2. Step 2 (capital safety) and step 3 (crash/restart safety) next — these
   are the two failure modes that would actually lose a user money or
   trust, so they're the real gate before calling this "ready for any
   user."
3. Steps 4-6 (abuse prevention, fees, marketplace semantics) once 1-3 are
   confirmed solid — these matter for scale/trust but aren't
   money-losing bugs on their own.
4. Step 7 (AI Agent re-enable) is intentionally last and out of scope for
   "make the existing bot system work properly" — it's a separate,
   larger feature (a real AI-driven strategy-configuration flow) layered
   on top of a bot system that, by that point, will already be solid.

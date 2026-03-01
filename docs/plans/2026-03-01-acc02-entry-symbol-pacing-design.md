# Acc02 Entry Symbol Pacing Design

**Goal:** Keep `acc02` on its existing `07:45` delayed entry schedule and add a `30s` wait after each processed entry symbol, while leaving all other accounts unchanged.

## Scope
- Add an account-level runtime setting for entry symbol pacing.
- Apply pacing only inside the main entry candidate loop.
- Count every processed symbol the same for pacing purposes: success, skip, and failure all trigger the wait before the next candidate.
- Do not change ranking, scheduling, rebalance, redistribution, or other account behavior.

## Recommended Approach
1. Add `entry_symbol_interval_sec` to account runtime config, defaulting to `0`.
2. Pass that value into each account's `Top10ShortStrategy` instance during component creation.
3. In `Top10ShortStrategy.run_entry()`, sleep after each processed candidate when:
   - the configured interval is greater than `0`, and
   - there is another candidate remaining.
4. Log the pacing event so production behavior is visible in logs.

## Why This Approach
- The requirement is symbol-level pacing, not account-level scheduling.
- `acc02` already has a dedicated `07:45` schedule, so the missing piece is only the per-symbol gap.
- Strategy-level pacing is the narrowest change and avoids altering shared ranking or runtime orchestration.

## Error Handling
- Pacing must occur even when a symbol is skipped or fails, because the requirement is based on "processed symbol" rather than successful order placement.
- The final symbol should not sleep after completion.
- If pacing is `0`, behavior remains exactly as before.

## Tests
- Verify config parsing exposes `entry_symbol_interval_sec` per account.
- Verify `run_entry()` sleeps between symbols when the account pacing is enabled.
- Verify it sleeps after failures/skips as well, but not after the last candidate.
- Verify default accounts without the setting do not sleep.

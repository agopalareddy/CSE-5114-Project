### 🔑 Composite Key for Pitch Data

To uniquely identify every pitch, we'll use a **composite key** by combining these three columns:
1.  `game_pk` (Game ID)
2.  `at_bat_number` (At-bat ID)
3.  `pitch_number` (Pitch ID)

The final, unique key (we can call it `pitch_uid`) will look like this: `game_pk_at_bat_number_pitch_number`.

**Example:** `717435_1_1`

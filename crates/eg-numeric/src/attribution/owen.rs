//! Owen values: Shapley values under a declared hierarchy. Players belong to unions
//! (for example pods within services); unions enter coalitions whole, in Shapley
//! order among unions, and a player's share is its Shapley value inside its union
//! given the unions already present. Owen values are efficient, and each union's total
//! equals the union's Shapley value in the game played between unions.

use std::collections::BTreeMap;

use super::shapley::{budget_refusal, members_of, shapley_weights};
use super::EXACT_MAX_PLAYERS;
use super::{invalid, Attribution, AttributionCode, AttributionError, AttributionResult, Game};

/// A memo of coalition values keyed by the ascending member list.
struct Memo<'g, G: Game + ?Sized> {
    game: &'g G,
    values: BTreeMap<Vec<usize>, f64>,
}

impl<G: Game + ?Sized> Memo<'_, G> {
    fn value(&mut self, mut members: Vec<usize>) -> AttributionResult<f64> {
        members.sort_unstable();
        if let Some(value) = self.values.get(&members) {
            return Ok(*value);
        }
        let value = self.game.value(&members)?;
        self.values.insert(members, value);
        Ok(value)
    }
}

fn check_partition(players: usize, unions: &[Vec<usize>]) -> AttributionResult<()> {
    let mut seen = vec![false; players];
    for member in unions.iter().flatten() {
        let slot = seen
            .get_mut(*member)
            .ok_or_else(|| invalid(format!("union member {member} is not a player")))?;
        if *slot {
            return Err(invalid(format!("player {member} is in two unions")));
        }
        *slot = true;
    }
    if seen.iter().any(|s| !s) || unions.iter().any(Vec::is_empty) {
        return Err(invalid("the unions must partition the players, none empty"));
    }
    Ok(())
}

fn check_sizes(unions: &[Vec<usize>], max_evaluations: u64) -> AttributionResult<()> {
    let too_big =
        unions.len() > EXACT_MAX_PLAYERS || unions.iter().any(|u| u.len() > EXACT_MAX_PLAYERS);
    if too_big {
        return Err(AttributionError::new(
            AttributionCode::TooManyPlayers,
            format!(
                "Owen values enumerate at most {EXACT_MAX_PLAYERS} unions of at most \
                     {EXACT_MAX_PLAYERS} players"
            ),
        ));
    }
    let outer = 1u64 << (unions.len() - 1);
    let needed: u64 = unions
        .iter()
        .map(|u| outer * (1u64 << (u.len() - 1)) * 2 * u.len() as u64)
        .sum();
    if needed > max_evaluations {
        return Err(budget_refusal(needed, max_evaluations));
    }
    Ok(())
}

/// The players of the unions selected by `mask` over `others` (union indices).
fn union_members(unions: &[Vec<usize>], others: &[usize], mask: usize) -> Vec<usize> {
    members_of(mask, others.len())
        .into_iter()
        .flat_map(|slot| unions[others[slot]].iter().copied())
        .collect()
}

/// One player's Owen value inside union `home`.
fn player_owen<G: Game + ?Sized>(
    memo: &mut Memo<'_, G>,
    unions: &[Vec<usize>],
    home: usize,
    player: usize,
) -> AttributionResult<f64> {
    let others: Vec<usize> = (0..unions.len()).filter(|&u| u != home).collect();
    let mates: Vec<usize> = unions[home]
        .iter()
        .copied()
        .filter(|&m| m != player)
        .collect();
    let outer = shapley_weights(unions.len());
    let inner = shapley_weights(unions[home].len());
    let mut acc = 0.0;
    for r_mask in 0..1usize << others.len() {
        let base = union_members(unions, &others, r_mask);
        let r_weight = outer[r_mask.count_ones() as usize];
        for t_mask in 0..1usize << mates.len() {
            let mut coalition = base.clone();
            coalition.extend(
                members_of(t_mask, mates.len())
                    .into_iter()
                    .map(|s| mates[s]),
            );
            let without = memo.value(coalition.clone())?;
            coalition.push(player);
            let with = memo.value(coalition)?;
            acc += r_weight * inner[t_mask.count_ones() as usize] * (with - without);
        }
    }
    Ok(acc)
}

/// Exact Owen values of `game` under `unions` (a partition of the players, at most
/// [`EXACT_MAX_PLAYERS`] unions of at most that many players each). Coalition values
/// are memoised, so `evaluations` counts distinct coalitions valued.
pub fn owen_values<G: Game + ?Sized>(
    game: &G,
    unions: &[Vec<usize>],
    max_evaluations: u64,
) -> AttributionResult<Attribution> {
    let players = game.players();
    if players == 0 {
        return Err(invalid("a game needs at least one player"));
    }
    check_partition(players, unions)?;
    check_sizes(unions, max_evaluations)?;
    let mut memo = Memo {
        game,
        values: BTreeMap::new(),
    };
    let mut phi = vec![0.0; players];
    for (home, union) in unions.iter().enumerate() {
        for &player in union {
            phi[player] = player_owen(&mut memo, unions, home, player)?;
        }
    }
    let empty = memo.value(Vec::new())?;
    let grand = memo.value((0..players).collect())?;
    Ok(Attribution {
        phi,
        half_width: None,
        grand,
        empty,
        evaluations: memo.values.len() as u64,
    })
}

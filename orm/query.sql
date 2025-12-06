-- name: CreateProposal :one
INSERT INTO proposals (id)
VALUES (?)
RETURNING *;

-- name: AddProposalMeter :exec
INSERT INTO proposal_meters (proposal_id, m3ter_no, account, nonce)
VALUES (?, ?, ?, ?);

-- name: GetProposalMeters :many
SELECT m3ter_no, account, nonce
FROM proposal_meters
WHERE proposal_id = ?;

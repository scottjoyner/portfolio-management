# Security Model

## Security Objectives

The system must protect customer data, regulated insurance decisions, treasury assets, smart contract authority, and audit integrity.

## Core Rules

1. No sensitive customer data on-chain.
2. No unaudited AI tool execution.
3. No direct production database access by AI agents.
4. No treasury execution without policy checks and approvals.
5. No smart contract admin actions without multisig or equivalent approval.
6. No policy binding without quote, risk appetite, payment, and authority checks.
7. No claim payout without coverage verification and approval thresholds.

## Identity and Access

Use RBAC plus ABAC.

### Roles

```text
CUSTOMER
PROSPECT
LICENSED_AGENT
PRODUCER
UNDERWRITER_L1
UNDERWRITER_L2
CLAIMS_ADJUSTER
CLAIMS_MANAGER
TREASURY_ANALYST
TREASURY_APPROVER
COMPLIANCE_OFFICER
ACTUARY
AUDITOR
ADMIN
SMART_CONTRACT_SIGNER
```

### Attributes

```text
jurisdiction
product_line
authority_limit
license_status
department
case_assignment
approval_threshold
chain_signing_role
```

## Data Protection

- Encrypt PII at rest.
- Encrypt policy documents and claim evidence.
- Use field-level access controls for sensitive records.
- Store document hashes for verification.
- Use object storage retention policies.
- Maintain access logs for regulated records.
- Separate customer identity from on-chain identifiers.

## AI Agent Security

AI agents must operate through permissioned tools only.

Agents must not:

- Execute raw SQL against production systems.
- Modify rating factors directly.
- Bind policies without authority.
- Approve adverse decisions without review thresholds.
- Execute treasury actions.
- Approve high-value claims.
- Override compliance blocks.
- Push on-chain transactions directly.

Every AI tool call must log:

- session ID
- actor ID
- model ID
- prompt/template version
- retrieved context
- tool name
- tool inputs
- tool outputs
- policy decision
- human review requirement

## Smart Contract Security

- Use least-privilege roles.
- Add emergency pause support.
- Use upgrade timelocks or immutable MVP contracts.
- Use multisig for admin and treasury authority.
- Treat oracle updates as privileged operations.
- Add replay protection for off-chain signatures.
- Test access control boundaries.
- Reconcile on-chain and off-chain state continuously.

## Treasury Security

Treasury operations must be policy-gated.

Required controls:

- approved asset list
- counterparty limits
- liquidity limits
- duration limits
- approval thresholds
- daily mark-to-market
- execution audit logs
- reserve segregation
- emergency freeze procedure

## Testing Requirements

Security tests must prove:

- PII is never included in chain payloads.
- Unauthorized users cannot bind policies.
- Unauthorized users cannot approve claims.
- Unauthorized users cannot execute treasury actions.
- AI agents cannot bypass tool permissions.
- Smart contract role checks cannot be bypassed.
- Chain write retries are idempotent.

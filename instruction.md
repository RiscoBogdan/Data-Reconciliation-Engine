# Engineering Instructions

This document defines mandatory engineering standards for this repository.
Apply these rules to all new and modified code.

## 1) Technology and Framework Requirements

- Use Spring Boot as the primary application framework.
- Prefer Spring-managed components and dependency injection over manual wiring.
- Use standard stereotypes by layer:
  - `@RestController` for API endpoints
  - `@Service` for business logic
  - `@Repository` for persistence access
  - `@Configuration` for explicit configuration beans
- Keep framework usage consistent with existing project architecture and package boundaries.

## 2) Coding Style and Quality

- Follow clean, readable, and maintainable Java code conventions.
- Keep methods small and single-purpose.
- Prefer clear naming over abbreviations.
- Avoid duplicated logic; extract shared behavior into focused helpers/services.
- Use immutable data structures and `record` types where appropriate.
- Validate method inputs early and fail fast with meaningful error messages.
- Handle exceptions explicitly; do not swallow errors.
- Write logs with actionable context, but never include secrets or sensitive data.
- Add or update tests for all non-trivial behavior changes.

## 3) System Design Principles

- Design for modularity and clear separation of concerns.
- Keep domain/business logic independent from transport and persistence concerns.
- Define stable interfaces at boundaries to reduce coupling.
- Favor composition over inheritance unless inheritance is clearly justified.
- Ensure components are observable (logging, metrics hooks, error visibility).
- Consider performance, scalability, and operability in design decisions.

## 4) Design Patterns (When Needed)

Use patterns intentionally to solve real problems, not by default.

- Strategy pattern for interchangeable algorithms or query behaviors.
- Factory/Builder for complex object creation and configuration safety.
- Adapter pattern for integrating external systems with mismatched interfaces.
- Template Method only when shared workflow with controlled extension points is needed.
- Repository pattern for persistence abstraction and testability.

Pattern usage must improve clarity, testability, and long-term maintainability.

## 5) Maximum Security Compliance (Mandatory)

- Treat security as a default requirement for every change.
- Follow secure-by-default principles and least privilege.
- Never hardcode credentials, API keys, tokens, or secrets.
- Load secrets from secure configuration sources (environment variables or secret managers).
- Validate and sanitize all external inputs (API, DB, files, configs).
- Use parameterized queries/prepared statements; never construct SQL from raw input.
- Enforce authentication and authorization checks at appropriate boundaries.
- Protect sensitive data in transit and at rest where applicable.
- Avoid logging PII, credentials, tokens, or connection strings with secrets.
- Keep dependencies updated; remediate known vulnerabilities quickly.
- Add security tests/checks for critical paths and high-risk changes.
- Fail closed on security-relevant errors.

## 6) Pull Request and Change Gates

A change is considered complete only if:

- Architecture remains layered and coherent.
- Code style and readability standards are met.
- Tests cover new/changed behavior.
- Security requirements in this document are satisfied.
- No new high or critical vulnerabilities are introduced.
- Documentation is updated when behavior or configuration changes.


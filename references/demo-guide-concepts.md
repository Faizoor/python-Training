# Demo Guide Concepts — Summary (Simple)

One-line takeaways from `lab/Day06_Demo_Guide.md` focused on pipeline OOP design.

- Contract (ABC) – Use an Abstract Base Class to enforce a consistent interface.
- Lifecycle methods – Typical readers implement `connect`, `read`, and `close`.
- Polymorphic runner – Design the pipeline to accept any object following the contract.
- Base class reuse – Put common connection logic in a base class to reduce duplication.
- Method overriding – Subclasses override `read` or `connect` for source-specific logic.
- Error safety – Ensure `close` runs (e.g., with `try/finally`) to release resources.
- Open/Closed Principle – Add new sources by extending classes, not changing the runner.
- Mocking for tests – Provide mock readers to test pipeline logic without external systems.
- CSV reader – Example of a file-backed DataSource implementation.
- DB reader – Example of a database-backed DataSource implementation.
- Separation of concerns – Keep source-specific code out of the pipeline runner.
- Design for swapability – Swap data sources by passing different reader implementations.

---

These bullets map directly to patterns and examples in the Day 06 demo guide.

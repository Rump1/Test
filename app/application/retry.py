from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    initial_delay: int = 1

    def __post_init__(self) -> None:
        if self.max_attempts < 1 or self.initial_delay < 1:
            raise ValueError("retry settings must be positive")

    def delay_after(self, attempt: int) -> int:
        return self.initial_delay * 2 ** (attempt - 1)

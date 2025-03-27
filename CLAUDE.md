# CLAUDE.md - Email Scheduler Project Guidelines

## Build and Test Commands
- Run all tests: `./run_tests.sh`
- Run specific test file: `uv run python -m unittest test_file.py`
- Run with verbose output: `uv run python -m unittest test_file.py -v`
- Run optimized version: `./run_with_uv.sh email_scheduler_optimized.py --input input.json --output output.json [--async] [--start-date YYYY-MM-DD]`
- Run SendGrid test: `./run_with_uv.sh test_sendgrid.py [--live]`
- Validate rule configs: `uv run python rule_config_validator.py`

## Code Style Guidelines
- **Imports**: Standard lib first, third-party next, sort alphabetically within groups
- **Types**: Use `from typing import Dict, List, Optional` consistently
- **Naming**: `UPPER_SNAKE` constants, `snake_case` variables/functions, `PascalCase` classes
- **Error Handling**: Use specific exceptions, log with context, recover gracefully
- **Formatting**: 4-space indentation, blank lines between logical sections
- **Documentation**: Docstrings for functions, comment complex logic
- **Async Patterns**: Use `async/await` consistently, handle exceptions properly
- **Testing**: Verify both sync/async implementations with test_compare.py

## Code Organization
The codebase uses the optimized implementation with legacy wrappers:
- `email_scheduler_optimized.py`: Main implementation (both sync/async)
- `contact_rule_engine.py`: Rule processing for contact-specific rules
- `email_template_engine.py`: Template generation system
- `send_scheduled_emails.py`: SendGrid integration for email delivery

## Email Rules
- Birthday emails: 14 days before birthday
- Effective date emails: 30 days before effective date
- AEP emails: Distributed across August/September weeks
- Post-window emails: Follow state-specific rules
- Special rules for leap years and state-specific timing
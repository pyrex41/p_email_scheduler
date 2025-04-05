# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands
- Run scheduler: `./run_with_uv.sh email_scheduler_optimized.py --input input.json --output output.json [--async] [--start-date YYYY-MM-DD]`
- Full pipeline: `./run_scheduler_and_send.sh --input contacts.json --output scheduled_emails.json [--async] [--live]`
- Run tests: `./run_with_uv.sh -m unittest test_email_scheduler.py`
- Run single test: `./run_with_uv.sh -m unittest test_email_scheduler.TestClassName.test_method_name`
- Validate templates: `./run_with_uv.sh email_template_engine.py --validate`

## Code Style Guidelines
- **Imports**: Standard lib first, third-party next, sort alphabetically within groups
- **Types**: Use type hints (`from typing import Dict, List, Optional, Any`) consistently
- **Naming**: `UPPER_SNAKE` constants, `snake_case` variables/functions, `PascalCase` classes
- **Error Handling**: Use specific exceptions, log with context, recover gracefully 
- **Formatting**: 4-space indentation, blank lines between logical sections
- **Documentation**: Docstrings for functions/classes, comment complex logic
- **Async**: Use `async/await` consistently, handle exceptions properly

## Code Organization
- `email_scheduler_optimized.py`: Main implementation (both sync/async)
- `contact_rule_engine.py`: Rule processing for contact-specific rules
- `email_template_engine.py`: Template generation system
- `send_scheduled_emails.py`: SendGrid integration for email delivery
- `app.py`: Web interface for system management

## Email Rules
- Birthday emails: 14 days before birthday
- Effective date emails: 30 days before effective date
- AEP emails: Distributed across August/September weeks
- Special rules for leap years and state-specific timing
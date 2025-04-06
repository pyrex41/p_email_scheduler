# Email Scheduler

A FastAPI web application for scheduling and sending Medicare-related emails based on complex business rules.

## Features

- Web interface for checking email schedules for an organization's contacts
- Sample contacts based on state and special rule criteria
- Support for state-specific email rules (birthday rules, effective date rules, etc.)
- Detailed display of scheduled emails with timing information
- Integration with SendGrid for email delivery
- Batch email sending capabilities for improved performance
- Environment-based configuration with .env file support

## Technical Stack

- FastAPI for the web backend
- Jinja2 for templating
- SQLite for data storage
- Async processing for handling large contact datasets
- Bootstrap for the frontend interface
- SendGrid for email delivery
- Python-dotenv for environment configuration

## Environment Setup

This project uses environment variables for configuration. For convenience, you can use a `.env` file.

1. Copy the example environment file to create your own:
   ```
   cp .env.example .env
   ```

2. Edit the `.env` file and customize the following variables:
   - `SENDGRID_API_KEY`: Your SendGrid API key for sending emails
   - `FROM_EMAIL`: Default sender email address
   - `FROM_NAME`: Default sender name
   - `EMAIL_DRY_RUN`: Set to "false" to actually send emails, "true" for dry-run mode
   - `EMAIL_SCHEDULER_BASE_URL`: Base URL for the application
   - `QUOTE_SECRET`: Secret key for generating quote links

## Running the Application

### Email Sending Control

Use the email sending control script to manage email sending settings:

```bash
# Dry run mode (no real emails sent)
./email_sending_control.sh --dry-run server

# Test mode (only sends to test email addresses)
./email_sending_control.sh --test-only server

# Live mode (sends real emails to recipients)
./email_sending_control.sh --live server
```

### Running Scripts

Use the UV runner script to run Python scripts with environment variables loaded:

```bash
./run_with_uv.sh email_scheduler_optimized.py --input data.json --output results.json
```

## Development

### Project Structure

- `app.py`: Web interface for system management
- `email_scheduler_optimized.py`: Main implementation (both sync/async)
- `contact_rule_engine.py`: Rule processing for contact-specific rules
- `email_template_engine.py`: Template generation system
- `sendgrid_client.py`: SendGrid integration for email delivery
- `email_batch_manager.py`: Manages email batches and sending process
- `dotenv_config.py`: Environment variable management

### Running Tests

```bash
./run_with_uv.sh -m unittest test_email_scheduler.py
```

## Environment Variables

The application uses the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SENDGRID_API_KEY` | SendGrid API key | None |
| `FROM_EMAIL` | Default sender email | medicare@example.com |
| `FROM_NAME` | Default sender name | Medicare Services |
| `EMAIL_DRY_RUN` | Dry run mode | true |
| `TEST_EMAIL_SENDING` | Allow test emails | ENABLED |
| `PRODUCTION_EMAIL_SENDING` | Allow production emails | DISABLED |
| `EMAIL_SCHEDULER_BASE_URL` | Base URL for links | https://maxretain.com |
| `QUOTE_SECRET` | Secret for quote links | your-default-secret-key |
| `LOG_FILE` | Log file path | logs/email_scheduler.log |

## State-Specific Rules

The application handles special Medicare rules for various states:
- Birthday rule states (CA, ID, IL, KY, LA, MD, NV, OK, OR)
- Effective date rule states (MO)
- Year-round enrollment states
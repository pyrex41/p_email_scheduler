"""
Email template engine for generating email content based on email type.
Uses Jinja2 for template rendering and YAML for metadata.
"""

import os
from datetime import datetime, date
from typing import Dict, Any, Optional
import jinja2
import yaml
import logging

logger = logging.getLogger(__name__)

class EmailTemplateEngine:
    def __init__(self, template_dir: str = 'templates'):
        """Initialize the template engine with template directories"""
        self.template_dir = template_dir
        self.text_dir = os.path.join(template_dir, 'text')
        self.html_dir = os.path.join(template_dir, 'html')
        
        # Create template directories if they don't exist
        os.makedirs(self.text_dir, exist_ok=True)
        os.makedirs(self.html_dir, exist_ok=True)
        
        # Initialize Jinja2 environments
        self.text_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(self.text_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        self.html_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(self.html_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Register custom filters
        self._register_filters()
    
    def _register_filters(self):
        """Register custom Jinja2 filters"""
        def format_date(value):
            if isinstance(value, str):
                try:
                    value = datetime.strptime(value, "%Y-%m-%d").date()
                except ValueError:
                    return value
            return value.strftime("%B %d, %Y")
        
        def format_phone(value):
            if not value:
                return ""
            # Remove all non-numeric characters
            nums = ''.join(filter(str.isdigit, str(value)))
            if len(nums) == 10:
                return f"({nums[:3]}) {nums[3:6]}-{nums[6:]}"
            elif len(nums) == 11 and nums[0] == '1':
                return f"({nums[1:4]}) {nums[4:7]}-{nums[7:]}"
            elif len(nums) == 7:
                return f"{nums[:3]}-{nums[3:]}"
            return value
        
        def format_currency(value):
            try:
                return "${:,.2f}".format(float(value))
            except (ValueError, TypeError):
                return value
        
        # Register filters for both environments
        for env in [self.text_env, self.html_env]:
            env.filters['date'] = format_date
            env.filters['phone'] = format_phone
            env.filters['currency'] = format_currency
    
    def _load_template_metadata(self, template_type: str) -> Dict[str, Any]:
        """Load metadata for a template type from YAML"""
        metadata_file = os.path.join(self.template_dir, f"{template_type}_metadata.yaml")
        try:
            with open(metadata_file, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"No metadata file found for {template_type}")
            return {}
        except Exception as e:
            logger.error(f"Error loading metadata for {template_type}: {e}")
            return {}
    
    def _get_template_vars(self, template_type: str, contact: Dict[str, Any], email_date: date) -> Dict[str, Any]:
        """Prepare variables for template rendering"""
        # Load template metadata
        metadata = self._load_template_metadata(template_type)
        
        # Basic contact info
        vars = {
            'contact': contact,
            'email_date': email_date,
            'first_name': contact.get('first_name', ''),
            'last_name': contact.get('last_name', ''),
            'state': contact.get('state', ''),
        }
        
        # Preserve quote_link if it exists in the contact data
        if 'quote_link' in contact:
            logger.info(f"Preserving quote_link from contact data: {contact['quote_link']}")
            vars['quote_link'] = contact['quote_link']
        else:
            # We'll handle this with defaults in the template, but log it as a warning
            logger.warning("No quote_link found in contact data")
        
        # Add organization data if present in the contact
        if 'organization' in contact:
            vars['organization'] = contact['organization']
            # Set fallback values from organization data if available
            company_name = contact['organization'].get('name', 'Medicare Services')
            phone = contact['organization'].get('phone', '1-800-MEDICARE')
            website = contact['organization'].get('website', 'www.medicare.gov')
        else:
            # Fallback defaults
            company_name = "Medicare Services"
            phone = "1-800-MEDICARE"
            website = "www.medicare.gov"
            
            # Create a default organization object to prevent template errors
            vars['organization'] = {
                'name': company_name,
                'phone': phone,
                'website': website,
                'primary_color': '#03045E',  # Default blue color
                'logo_data': None
            }
        
        # Set company data for backwards compatibility
        vars['company_name'] = company_name
        vars['phone'] = phone
        vars['website'] = website
        
        # Add metadata variables
        vars.update(metadata.get('variables', {}))
        
        # Add type-specific variables
        if template_type == 'birthday':
            birth_date = datetime.strptime(contact['birth_date'], "%Y-%m-%d").date() if isinstance(contact['birth_date'], str) else contact['birth_date']
            vars['birth_date'] = birth_date
            vars['birth_month'] = birth_date.strftime("%B")
            
        elif template_type == 'anniversary' or template_type == 'effective_date':
            if contact.get('effective_date'):
                effective_date = datetime.strptime(contact['effective_date'], "%Y-%m-%d").date() if isinstance(contact['effective_date'], str) else contact['effective_date']
                vars['effective_date'] = effective_date
            
        elif template_type == 'aep':
            vars['aep_start'] = date(email_date.year, 10, 15)
            vars['aep_end'] = date(email_date.year, 12, 7)
        
        # Check if quote_link is in the final template variables
        if 'quote_link' in vars:
            logger.info(f"quote_link is in the final template variables: {vars['quote_link']}")
        else:
            logger.warning("quote_link is not in the final template variables")
            
        return vars
    
    def render_email(self, template_type: str, contact: Dict[str, Any], email_date: date, html: bool = False) -> Dict[str, str]:
        """
        Render an email template
        
        Args:
            template_type: Type of email template (birthday, effective_date, aep, post_window)
            contact: Contact information dictionary
            email_date: Date the email will be sent
            html: Whether to render HTML version (default: False)
        
        Returns:
            Dictionary with subject and body/html keys
        """
        # Prepare template variables
        template_vars = self._get_template_vars(template_type, contact, email_date)
        
        # Log the template variables to check if quote_link is present
        logger.info(f"Template variables keys: {template_vars.keys()}")
        logger.debug(f"Template variables content: {template_vars}")
        
        if 'organization' in template_vars:
            logger.debug(f"Organization data: {template_vars['organization']}")
            if isinstance(template_vars['organization'], dict):
                logger.debug(f"Organization primary_color: {template_vars['organization'].get('primary_color')}")
        
        if 'quote_link' in template_vars:
            logger.info(f"Quote link in template variables: {template_vars['quote_link']}")
        else:
            logger.warning("quote_link not found in template variables")
        
        # Get metadata for subject line
        metadata = self._load_template_metadata(template_type)
        subject = metadata.get('subject', f"{template_type.title()} Email for {contact.get('first_name', '')}")
        
        try:
            # Render subject line with template vars
            logger.debug("Attempting to render subject line")
            subject = self.text_env.from_string(subject).render(**template_vars)
            
            if html:
                # Render HTML template with template vars
                logger.debug(f"Loading HTML template for {template_type}")
                template = self.html_env.get_template(f"{template_type}/email.html")
                logger.debug("Attempting to render HTML template")
                content = template.render(**template_vars)
                logger.debug("Successfully rendered HTML template")
                return {
                    'subject': subject,
                    'html': content
                }
            else:
                # Render text template with template vars
                logger.debug(f"Loading text template for {template_type}")
                text_template = self.text_env.get_template(f"{template_type}/email.txt")
                logger.debug("Attempting to render text template")
                body = text_template.render(**template_vars)
                logger.debug("Successfully rendered text template")
                return {
                    'subject': subject,
                    'body': body
                }
        except Exception as e:
            logger.error(f"Error rendering {template_type} template: {e}")
            logger.error(f"Template variables at time of error: {template_vars}")
            if html:
                return {
                    'subject': f"Error: {template_type.title()} Email",
                    'html': f"<p>Error rendering template: {e}</p>"
                }
            else:
                return {
                    'subject': f"Error: {template_type.title()} Email",
                    'body': f"Error rendering template: {e}"
                }
    
    def preview_email(self, template_type: str, contact: Dict[str, Any], email_date: date):
        """Preview both text and HTML versions of an email"""
        print(f"\nPreviewing {template_type} email for {contact.get('first_name')} {contact.get('last_name')}")
        print("-" * 80)
        
        # Render text version
        text_result = self.render_email(template_type, contact, email_date)
        print(f"Subject: {text_result['subject']}")
        print("\nText Content:")
        print(text_result['body'])
        
        # Render HTML version
        print("\nHTML Content:")
        html_result = self.render_email(template_type, contact, email_date, html=True)
        print(html_result)
    
    def validate_templates(self) -> bool:
        """Validate that all required templates exist and can be rendered"""
        template_types = ['birthday', 'effective_date', 'aep', 'post_window']
        success = True
        
        for template_type in template_types:
            # Check text template
            text_path = os.path.join(self.text_dir, template_type, 'email.txt')
            if not os.path.exists(text_path):
                logger.error(f"Missing text template: {text_path}")
                success = False
            
            # Check HTML template
            html_path = os.path.join(self.html_dir, template_type, 'email.html')
            if not os.path.exists(html_path):
                logger.error(f"Missing HTML template: {html_path}")
                success = False
            
            # Check metadata
            metadata_path = os.path.join(self.template_dir, f"{template_type}_metadata.yaml")
            if not os.path.exists(metadata_path):
                logger.error(f"Missing metadata file: {metadata_path}")
                success = False
        
        return success 
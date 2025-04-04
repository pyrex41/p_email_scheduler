from test_email_scheduler import *
from pprint import pprint
import asyncio

ar = asyncio.run

e = EmailScheduler()

current_date = date(2025, 3, 31)
end_date = date(2027, 12, 31)


oc = load_org_contacts(37)

d = format_contact_data(oc)

r = lambda x: e.process_contact(d[x], current_date, end_date)

# t_list = is random list of 10 contact_ids
t_list = random.sample(range(len(d)), 10)
d_list = [r(i) for i in t_list]
pprint(d_list)

dd = ar(main_async(d, current_date, end_date,1000))
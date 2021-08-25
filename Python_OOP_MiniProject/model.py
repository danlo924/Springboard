class Customer:
    """
    Bank Customers
    """
    def __init__(self, first_name, last_name, address, account_num=None):
        self.first_name = first_name
        self.last_name = last_name
        self.address = address
        self.account_num = account_num

class Employees:
    """
    Bank Employees
    """
    def __init__(self, first_name, last_name, title):
        self.first_name = first_name
        self.last_name = last_name
        self.title = title

class Account:
    """
    Bank Accounts
    """
    def __init__(self, acct_type, cust_id):
        self.acct_type = acct_type
        self.cust_id = cust_id
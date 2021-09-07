from datetime import datetime
import random as r

class BankBranch:
    """
    Bank Branch Information
    """
    def __init__(self, branch_name, address):
        self.branch_name = branch_name
        self.address = address

class Customer:
    """
    Bank Customers. Each new customer is assigned a random Customer ID which is a 6-digit number, starting with 1
    """
    def __init__(self, first_name, last_name, address, phone_num, ssn):
        self.first_name = first_name
        self.last_name = last_name
        self.address = address
        self.phone_num = phone_num
        self.ssn = ssn
        self.cust_id = r.randint(100000,200000)
    
    def greet_new_customer(self):
        print("Welcome {}! I have just entered all of your information into our system.".format(self.first_name))
        print("Your Customer ID is: {}".format(self.cust_id))
    

class Account:
    """
    All Bank Accounts. Each new bank account is assigned a random Account Number which is a 9-digit number, starting with 2
    """
    def __init__(self, acct_type, cust_id):
        self.acct_type = acct_type
        self.cust_id = cust_id
        self.acct_num = r.randint(200000000,300000000)
        self.open_date = datetime.now()
        self.close_date = None
        self.balance = 0.00

    def new_acct_msg(self):
        print("I have created your {} account.".format(self.acct_type))
        print("Your {} Account Number is: {}".format(self.acct_type, self.acct_num))

    def get_balance(self):
        print("Here's your balance: ${}".format(self.balance)) 

    def deposit(self, dep_amt):
        self.balance += float(dep_amt)
        print("I have completed your deposit. Your new balance is: ${}".format(self.balance))

    def withdraw(self, wth_amt):
        self.balance -= float(wth_amt)
        print("I have completed your withdrawal. Your new balance is: ${}".format(self.balance))

    def close_acct(self):
        self.close_date = datetime.now()
        print("I have closed your account.")

class CheckingAccount(Account):
    """
    Checking Accounts. This is the most basic account, and it's unique feature is charging overdraft fees.
    """
    def __init__(self, acct_type, cust_id):
        super().__init__(acct_type, cust_id)

    def new_acct_msg(self):
        super().new_acct_msg()
        print("There is $5 charge for each overdraft transaction on the checking account.")

class SavingsAccount(Account):
    """
    Savings Accounts. Savings accounts accrue interest.
    """
    def __init__(self, acct_type, cust_id):
        super().__init__(acct_type, cust_id)
        self.interest_rate = 0.25

    def new_acct_msg(self):
        super().new_acct_msg()
        print("The interest rate on your savings account is {}%.".format(self.interest_rate))

class Loan(Account):
    """
    Loan Accounts. A customer will be asked how much they want to borrow, and for how many months. Interest rates will be assigned based on the lenght of the loan.
    """
    def __init__(self, acct_type, cust_id, loan_amt, loan_length_mos):
        super().__init__(acct_type, cust_id)
        self.balance = loan_amt
        self.loan_length_mos = loan_length_mos
        if loan_length_mos <= 12:
            self.interest_rate = 3.00
        elif 12 < loan_length_mos <= 36:
            self.interest_rate = 4.00
        elif 36 < loan_length_mos <= 120:
            self.interest_rate = 5.00
        else:
            self.interest_rate = 6.00

    def new_acct_msg(self):
        super().new_acct_msg()
        print("The interest rate on your loan is {}%.".format(self.interest_rate))
        print("You have {} months to pay back your loan amount of ${}.".format(self.loan_length_mos, self.balance))

    def payment(self, pmt_amt):
        self.balance -= float(pmt_amt)
        print("I have completed your loan payment. Your new loan balance after payment is: ${}".format(self.balance))


class BankEmployee:
    """
    Bank Employees Information
    """
    def __init__(self, employee_id, first_name, last_name, title):
        self.employee_id = employee_id
        self.first_name = first_name
        self.last_name = last_name
        self.title = title

class BranchManager(BankEmployee):
    """
    Bank employees who are branch managers have more priviledges including the ability to open or close an account
    """
    pass

class Teller(BankEmployee):
    """
    Tellers handle the initial interaction with Customers; they are able to perform deposits / withdrawals, but must refer to BranchManagers for other tasks
    """
    pass
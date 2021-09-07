import module as m         #this contains the Class definitions and methods

# procedure for account opening, used by new and existing customers
def open_acct(cust_id):
    """
    This is the procedure for opening a new checking, savings, or loan account, for new and existing customers
    """
    global account_opening
    global customer_session
    acct_type = input("What type of account would you like to open? (checking/savings/loan): ")
    if acct_type in ("checking","savings","loan"):
        if acct_type == "checking":
            new_acct = m.CheckingAccount(acct_type, cust_id)
        elif acct_type == "savings":
            new_acct = m.SavingsAccount(acct_type, cust_id)
        elif acct_type == "loan":
            ask_borrow = input("How much would you like to borrow?: ")
            ask_term = input("How many months would you like to take to pay back the ${} loan?: ".format(ask_borrow))
            new_acct = m.Loan(acct_type, cust_id, float(ask_borrow), int(ask_term))
        new_acct.new_acct_msg()
        account_opening = False
    elif acct_type == "done":
        account_opening = False
        customer_session = False
    else:
        print("'{}' is not a valid account type. Please enter a valid account type (checking/savings/loan), or enter 'done'.".format(acct_type))

# begin customer interaction here:
customer_session = True
print("Hello! Welcome to Dan's Bank!")
# determine whether customer already has an account
ask_acct = input("Do you have an account with us? (Y/N): ")

# if customer doesn't currently have an account, give them an option to open an account and obtain a new Customer ID
if ask_acct == "N":
    ask_open_acct = input("Would you like to open an account with us? (Y/N): ")
    if ask_open_acct == "Y": 
        account_opening = True
        cust_first_name = input("What is your first name?: ")
        cust_last_name = input("What is your last name?: ")
        cust_addr = input("What is your address?: ")
        cust_phone = input("What is your phone number?: ")
        cust_ssn = input("What is your SSN?: ")
        new_cust = m.Customer(cust_first_name, cust_last_name, cust_addr, cust_phone, cust_ssn)
        new_cust.greet_new_customer() 
        while account_opening:
            open_acct(new_cust.cust_id)
    else:
        customer_session = False

# for new and existing customers, the first step before performing any transactions is to enter their Customer ID
if customer_session:
    cust_id = input("Please enter your Customer ID: ")

# the customer session should give customers the ability do to any transactions on their accounts, including opening / closing an account, until they are 'done'
while customer_session:
    ask_transaction = input("What would you like to do?\n Options: (balance/deposit/withdrawal/payment/open_acct/close_acct)\n Otherwise, if you are done, enter (done): ")
    if ask_transaction in ("balance","deposit","withdrawal","payment","close_acct"):
        acct_type = input("Great, I can help you with your {}. Please tell specify the type of account (checking/savings/loan): ".format(ask_transaction))
        acct_num = input("Please enter the {} Account Number: ".format(acct_type))
    if ask_transaction == "balance":
        acct_obj = m.Account(acct_type, cust_id)
        acct_obj.get_balance()
    elif ask_transaction == "deposit":
        acct_obj = m.Account(acct_type, cust_id)
        ask_dep_amt = input("How much would you like to deposit?: ")
        acct_obj.deposit(ask_dep_amt)
    elif ask_transaction == "withdrawal":
        acct_obj = m.Account(acct_type, cust_id)
        ask_wth_amt = input("How much would you like to withdraw?: ")
        acct_obj.withdraw(ask_wth_amt)
    elif ask_transaction == "payment":
        acct_obj = m.Loan(acct_type, cust_id, 500000.00, 360)
        ask_pmt_amt = input("How much would you like to pay?: ")
        acct_obj.payment(ask_pmt_amt)
    elif ask_transaction == ("close_acct"):
        acct_obj = m.Account(acct_type, cust_id)
        acct_obj.close_acct()
    elif ask_transaction == ("open_acct"):
        account_opening = True
        while account_opening:
            open_acct(cust_id)
    elif ask_transaction == "done":
        customer_session = False
    else:
        print("Sorry, I don't understand '{}'. Please tell me a valid transaction type.".format(ask_transaction))

# final greeting at the end of the customer session
print("Thank you, and have a great day! Goodbye!")
        
        
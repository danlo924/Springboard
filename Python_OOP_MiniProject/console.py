import model      ##why doesn't this work?
# from model import Customer

print("Hello! Welcome to the Bank!")

cust_input = input("Do you have an account with us? (Y/N): ")

# if cust_input == "N":  ## create new customer


cust1 = Customer("Dan","Wilde","Skyview St","123456789")
print(cust1.first_name)
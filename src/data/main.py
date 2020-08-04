import src.data.queries
import psycopg2
from src.data.config import config

while True:

    print("Welcome to the PostgreSQL database tool!")
    print("Which action would you like to perform?")
    print("1. Insert new certificate")
    print("2. Update existing certificate")
    print("3. Delete certificate")
    print("4. Create new person")
    print("5. Update existing person")
    print("6. Delete person")
    print("7. Quit program")

    choice = input("Please make your selection:")

    if choice == "1":

        certname = input("Please enter certificate name: ")
        src.data.queries.get_person()
        certid = input("Please enter certificate holder ID: ")

        try:
            if src.data.queries.check_cert_userid(certid)[0] != 0:
                src.data.queries.insert_certificate_row(certname, certid)
                print("Success!")
                continue

            else:
                raise Exception

        except:
            print("There was an error, returning to main menu")
            continue

    elif choice == "2":
        pass
    elif choice == "3":
        pass
    elif choice == "4":
        pass
    elif choice == "5":
        pass
    elif choice == "6":
        pass
    elif choice == "7":
        break

    else:
        print("Choose a valid value!")


    break

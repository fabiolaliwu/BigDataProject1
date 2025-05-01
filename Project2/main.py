from queries import Queries

#create an instamce for the class and call the query1 function
q = Queries()

while True:
    queryNumber = int(input("\nQuery 1: Top 5 compounds with most genes associated, and their associated diseases\nQuery 2: Top 5 diseases with drugs associated\nQuery 3: Names of top 5 compounds with most genes associate.\nPress 0: EXIT\nEnter the query number: "))
    if queryNumber in [1, 3]:
        q.query(queryNumber)
    elif queryNumber == 2:
        n = int(input("Enter the number of drugs: "))
        q.query2(n)
    else:
        print("Invalid query number. Please try again.")
        break


    

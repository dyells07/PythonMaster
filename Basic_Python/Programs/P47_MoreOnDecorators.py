
class BankAccount(object):
    def __init__(self, firstName, lastName):
        self.firstName = firstName
        self.lastName = lastName

    @property                   # property decorator
    def fullName(self):
        return self.firstName + ' ' + self.lastName

    @fullName.setter
    def fullName(self, name):
        firstName, lastName = name.split(' ')
        self.firstName = firstName
        self.lastName = lastName

if __name__ == '__main__':
    acc = BankAccount('Bipin', 'Khanal')
    print(acc.fullName)              # Notice that we can access the method for our class BankAccount without
                                     # parenthesis! This is beacuse of property decorator

    acc.fullName = 'Bhesraj Khanal'
    print(acc.fullName)

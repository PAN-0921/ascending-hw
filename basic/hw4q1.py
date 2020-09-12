#implement __hash__() and __eq__()
class User:

	def __init__(self, age, firstname):
		self.age=age
		self.firstname=firstname

	def __hash__(self):
	#prime=31
	#return age+31
		return hash(age)

	def __eq__(self, other):
		if not isinstance(other, User):
			return False
		return self.age==other.age


dict={}
dict['user1']='100'
dict['user2']='50'
dict['user3']='101'

user1=User(36,'a')
user2=User(20,'b')
user3=User(36,'c')

print(dict)

#leetcode_baseball game
#use stack
class Totalpoints(object):
	def points(self,strings_type):
		stack=[]
		for i in strings_type:
			if i=='C':
				stack.pop()
			elif i=='D':
				stack.append(2*stack[-1])
			elif i=='+':
				stack.append(stack[-1]+stack[-2])
			else:
				stack.append(int(i))		
		return sum(stack)

stack=[5,-2,4,'C','D',9,'+','+']
a=Totalpoints()
print(a.points(stack))

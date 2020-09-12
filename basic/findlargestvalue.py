# -*- coding:utf8 -*-
#Find Largest Value in Each Tree Row

class Node():
	def __init__(self,val):
		self.val=val
		self.left=None
		self.right=None

a=Node(1)
b=Node(3)
c=Node(2)
d=Node(5)
e=Node(3)
f=Node(9)

a.left=b
a.right=c
b.left=d
b.right=e
c.left=None
c.right=f


#def中有两个循环：while循环和for循环
#while循环是为了进入到下一层
#for循环是为了1.把本层所有数字pop出来；2.找出一层中最大的数值并赋值给tmax；3.把下一层中的所有数字append进去
#当for循环里面的数字都过了一遍全部完成之后才会开始下一次的while循环，此时为了保证找的是每一层中的tmax，应该在开始新的while循环的时候重置tmax，因此应该将tmax放入while循环里面for循环外面（位置很重要，会影响程序运行的结果）
#因为每一层都需要一个tmax，所以在完成一次完整的for循环之后需要将tmax的值放入re[]中。因此，re.append放在for循环之外，while循环里面
#当所有层数走完，len（que）等于0的时候，while循环结束，此时return re[]。因此，return re[]在while循环之外
#root=None为极端情况，做题时需要考虑


def findlargestvalue(root):
	if root==None:
		return []
	que=[root]
	re=[]
	
	while len(que) > 0:
		tmax=float('-inf')
		size=len(que)

		for i in range(size):
			t=que.pop(0)
			tmax=max(tmax,t.val)

			if t.left!=None:
				que.append(t.left)
			if t.right!=None:
				que.append(t.right)
		
		re.append(tmax)
	return re

print findlargestvalue(b)

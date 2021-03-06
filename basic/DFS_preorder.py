#hw4
#Binary Tree(DFS)_preorder
class Node(object):
	def __init__(self,val):
		self.val=val
		self.right=None
		self.left=None

a=Node(1)
b=Node(2)
c=Node(3)
d=Node(4)
e=Node(5)
a.left=b
a.right=c
b.left=d
b.right=e

def preorder(node):
	if node:
		return [node.val]+preorder(node.left)+preorder(node.right) 
	else:	
		return[]

print(preorder(a))

# -*- coding: utf-8 -*-
#Lowest Common Ancestor of a Binary Tree

#root指的是二叉树的最顶端
#在if中使用lowestcommonancestor时会return，这个return终结的是刚调用出来的lowestcommonancestor而不是最初的lowestcommonancestor
#整体思路：root一定是commonancestor，但是不一定是最小的commonancestor。因此，当root.right返回为None时，说明p和q不在right这一分支中，必然都在left这一分支中，因此return left。若right和left都有返回值，说明left和right分别包括p和q中的一个值，则他们的最小commonancestor必然是root。



class Node():
	def __init__(self,val):
		self.val=val
		self.left=None
		self.right=None

a=Node(3)
b=Node(5)
c=Node(1)
d=Node(6)
e=Node(2)
f=Node(0)
g=Node(8)
h=Node(7)
i=Node(4)

a.left=b
a.right=c
b.left=d
b.right=e
c.left=f
c.right=g
e.left=h
e.right=i

def lowestcommonancestor(root,p,q):
	if root==q or root==p:
		return root
	
	left=None
	right=None
	if root.left:
		left=lowestcommonancestor(root.left,p,q)
	if root.right:
		right=lowestcommonancestor(root.right,p,q)
	if left and right:
		return root
	else:
		return left or right

print(lowestcommonancestor(a,c,i).val)


class testobj:
    def __init__(self, val):
        self.val = val

class biggerobj:
    def __init__(self, testobj):
        self.t = testobj

a = testobj(5)
x = testobj(6)

b = biggerobj(a)
c = biggerobj(b.t)
b.t = x
print(c.t.val)




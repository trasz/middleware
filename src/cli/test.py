def ASTObject(name, *args):
        def str(self):
            return "<{0} {1}>".format(
                name,
                ' '.join(["{0} '{1}'".format(i, getattr(self, i)) for i in args])
            )

        def init(self, *values):
            for idx, i in enumerate(values):
                setattr(self, args[idx], i)

        dct = {k: None for k in args}
        dct['__init__'] = init
        dct['__str__'] = str
        dct['__repr__'] = str
        return type(name, (), dct)


Symbol = ASTObject('Symbol', 'name')
Set = ASTObject('Set', 'value')
BinaryExpr = ASTObject('BinaryExpr', 'left', 'op', 'right')

a = BinaryExpr('foo', '=', 'bar')
print a

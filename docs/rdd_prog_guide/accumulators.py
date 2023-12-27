accum = sc.accumulator(0)
print(accum)  # Accumulator<id=0, value=0>
sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
print(accum.value)  # 10


class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, initial_val):
        return Vector.zeros(initial_val.size)

    def addInPlace(self, v1, v2):
        v1 += v2
        return v1  # ? why not just: return v1 + v2?


vec_accum = sc.accumulator(Vector(...), VectorAccumulatorParam())


def g(x):
    accum.add(x)
    return f(x)


data.map(g)

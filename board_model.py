"""Compare low-dimensional, nonnegative wall-time models on scored releases."""
import math
from statistics import mean, median

MODELS = ("constant", "wall ratio", "wall affine", "CPU offset", "CPU affine",
          "CPU plus wait", "CPU and wait scales")


def features(row):
    # Average service time per available logical CPU, and remaining elapsed time.
    wall, cpu_seconds = row[:2]
    cpu = cpu_seconds / 8
    return wall, cpu, max(0.0, wall - cpu)


def nonnegative_fit(xs, ys):
    """Least squares in two dimensions including both nonnegative boundaries."""
    xx = sum(x*x for x, _ in xs)
    zz = sum(z*z for _, z in xs)
    xz = sum(x*z for x, z in xs)
    xy = sum(x*y for (x, _), y in zip(xs, ys))
    zy = sum(z*y for (_, z), y in zip(xs, ys))
    choices = [(max(0, xy/xx) if xx else 0, 0),
               (0, max(0, zy/zz) if zz else 0)]
    determinant = xx*zz-xz*xz
    if determinant > 1e-12*xx*zz:
        a, b = (xy*zz-zy*xz)/determinant, (zy*xx-xy*xz)/determinant
        if min(a,b) >= 0:
            choices.append((a,b))
    return min(choices, key=lambda p: sum((y-p[0]*x-p[1]*z)**2
                                          for (x,z),y in zip(xs,ys)))


def fit(name, rows):
    """Return coefficients in the order fixed seconds, CPU, uncovered wait."""
    ys = [row[2] for row in rows]
    fs = [features(row) for row in rows]
    if name == "constant":
        return mean(ys), 0, 0
    if name == "wall ratio":
        slope = median(y/w for y,(w,_,_) in zip(ys,fs))
        return 0, slope, slope
    if name == "wall affine":
        a,b = nonnegative_fit([(1,w) for w,_,_ in fs],ys)
        return a,b,b
    if name == "CPU offset":
        return max(0, mean(y-c for y,(_,c,_) in zip(ys,fs))), 1, 0
    if name == "CPU affine":
        a,b = nonnegative_fit([(1,c) for _,c,_ in fs],ys)
        return a,b,0
    if name == "CPU plus wait":
        a,b = nonnegative_fit([(1,s) for _,_,s in fs],
                              [y-c for y,(_,c,_) in zip(ys,fs)])
        return a,1,b
    if name == "CPU and wait scales":
        b,c = nonnegative_fit([(c,s) for _,c,s in fs],ys)
        return 0,b,c
    raise ValueError(name)


def estimate(parameters, sample):
    a,b,c = parameters
    _,cpu,stall = features(sample)
    return a+b*cpu+c*stall


def select(rows):
    results = []
    for name in MODELS:
        errors = [estimate(fit(name, rows[:i]+rows[i+1:]), row)-row[2]
                  for i,row in enumerate(rows)]
        results.append((mean(map(abs, errors)), math.sqrt(mean(e*e for e in errors)),
                        name, errors, fit(name,rows)))
    return sorted(results)

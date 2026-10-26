"""Simple Gorila (General Reinforcement Learning Architecture) demo.

Gorila = parameter server + N bundles. Each bundle is an actor, a replay
buffer and a learner. Actors act in the environment with their own copy of
the Q-network (theta) and fill replay memory. Learners sample from replay,
compute DQN gradients against a target network (theta-), and push them
asynchronously to the parameter server, which applies them to the master theta.

Everything runs in threads on one machine; the Q-network is a table (a linear
network on one-hot states) so the focus stays on the architecture.

Run:  python gorila_demo.py                   # local replay per bundle
      python gorila_demo.py --shared-buffer   # one replay shared by all bundles
"""
import argparse
import random
import threading
import time
from collections import deque

import numpy as np

SIZE = 5                      # 5x5 gridworld, start top-left, goal bottom-right
N_STATES, N_ACTIONS = SIZE * SIZE, 4          # up, down, left, right
MOVES = [(-1, 0), (1, 0), (0, -1), (0, 1)]
MAX_STEPS = 50


class GridWorld:
    """Reward +1 at goal, -0.01 per step otherwise."""

    def reset(self):
        self.pos = (0, 0)
        self.t = 0
        return self._s()

    def _s(self):
        return self.pos[0] * SIZE + self.pos[1]

    def step(self, a):
        r, c = self.pos
        dr, dc = MOVES[a]
        self.pos = (min(max(r + dr, 0), SIZE - 1), min(max(c + dc, 0), SIZE - 1))
        self.t += 1
        at_goal = self.pos == (SIZE - 1, SIZE - 1)
        done = at_goal or self.t >= MAX_STEPS
        return self._s(), (1.0 if at_goal else -0.01), at_goal, done


class ParameterServer:
    """Holds master theta; applies gradients from learners as they arrive."""

    def __init__(self, lr):
        self.theta = np.zeros((N_STATES, N_ACTIONS))
        self.version = 0
        self.lr = lr
        self.lock = threading.Lock()
        self.staleness = []                 # version gap of every applied gradient

    def get(self):
        with self.lock:
            return self.theta.copy(), self.version

    def push(self, grad, based_on_version):
        with self.lock:
            self.staleness.append(self.version - based_on_version)
            self.theta -= self.lr * grad
            self.version += 1


class ReplayBuffer:
    def __init__(self, capacity=5000):
        self.data = deque(maxlen=capacity)
        self.lock = threading.Lock()

    def add(self, tr):
        with self.lock:
            self.data.append(tr)

    def sample(self, n):
        with self.lock:
            return random.sample(self.data, min(n, len(self.data)))

    def __len__(self):
        return len(self.data)


def actor(ps, buf, stop, args, seed):
    rng = random.Random(seed)
    env, theta, step = GridWorld(), ps.get()[0], 0
    s = env.reset()
    while not stop.is_set():
        if step % args.actor_sync == 0:
            theta, _ = ps.get()                          # refresh local theta
        eps = max(0.05, 1.0 - step / 2000)
        a = rng.randrange(N_ACTIONS) if rng.random() < eps else int(np.argmax(theta[s]))
        s2, r, terminal, done = env.step(a)
        buf.add((s, a, r, s2, terminal))
        s = env.reset() if done else s2
        step += 1
        time.sleep(args.actor_delay)                     # pretend env steps cost time


def learner(ps, buf, stop, args, counter):
    theta, version = ps.get()
    theta_minus = theta.copy()                           # target network
    n = 0
    while not stop.is_set():
        if len(buf) < args.batch:
            time.sleep(0.01)
            continue
        if n % args.learner_sync == 0:
            theta, version = ps.get()                    # refresh online theta
        if n % args.target_sync == 0:
            theta_minus = ps.get()[0]                    # refresh theta-
        grad = np.zeros_like(theta)
        for s, a, r, s2, terminal in buf.sample(args.batch):
            y = r if terminal else r + args.gamma * theta_minus[s2].max()
            grad[s, a] += (theta[s, a] - y) / args.batch  # d/dtheta 0.5*(Q-y)^2
        ps.push(grad, version)
        n += 1
        with counter["lock"]:
            counter["n"] += 1
            if counter["n"] >= args.updates:
                stop.set()
        time.sleep(args.learner_delay)


def greedy_steps(theta):
    env, s = GridWorld(), GridWorld().reset()
    env = GridWorld()
    s = env.reset()
    for t in range(MAX_STEPS):
        s, _, goal, done = env.step(int(np.argmax(theta[s])))
        if goal:
            return t + 1
        if done:
            break
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bundles", type=int, default=4)
    p.add_argument("--updates", type=int, default=3000, help="total gradient pushes")
    p.add_argument("--shared-buffer", action="store_true")
    p.add_argument("--lr", type=float, default=0.5)
    p.add_argument("--gamma", type=float, default=0.95)
    p.add_argument("--batch", type=int, default=32)
    p.add_argument("--actor-sync", type=int, default=20, help="actor steps between theta refreshes")
    p.add_argument("--learner-sync", type=int, default=5, help="learner updates between theta refreshes")
    p.add_argument("--target-sync", type=int, default=50, help="learner updates between theta- refreshes")
    p.add_argument("--actor-delay", type=float, default=0.0005)
    p.add_argument("--learner-delay", type=float, default=0.001)
    p.add_argument("--seed", type=int, default=0)
    args = p.parse_args()
    random.seed(args.seed)

    ps, stop = ParameterServer(args.lr), threading.Event()
    counter = {"n": 0, "lock": threading.Lock()}
    shared = ReplayBuffer() if args.shared_buffer else None
    threads = []
    for i in range(args.bundles):                        # one bundle = actor + buffer + learner
        buf = shared or ReplayBuffer()
        threads.append(threading.Thread(target=actor, args=(ps, buf, stop, args, args.seed + i)))
        threads.append(threading.Thread(target=learner, args=(ps, buf, stop, args, counter)))

    mode = "shared" if args.shared_buffer else "local"
    print(f"Gorila demo: {args.bundles} bundles, {mode} replay, {args.updates} updates")
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0

    st = np.array(ps.staleness)
    steps = greedy_steps(ps.theta)
    print(f"time:       {elapsed:.1f}s")
    print(f"staleness:  mean {st.mean():.2f}, max {st.max()} (gradients applied after N other updates)")
    print(f"greedy policy reaches goal in {steps} steps (optimal = {2 * (SIZE - 1)})"
          if steps else "greedy policy does not reach the goal")
    arrows = "^v<>"
    for r in range(SIZE):
        print(" ".join("G" if (r, c) == (SIZE - 1, SIZE - 1) else arrows[int(np.argmax(ps.theta[r * SIZE + c]))]
                       for c in range(SIZE)))


if __name__ == "__main__":
    main()

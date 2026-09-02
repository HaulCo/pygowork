# Security policy

pygowork talks to your Redis with the credentials you hand it and executes
the Lua scripts carried from gocraft/work; it exposes no network surface of
its own. Still, if you believe you have found a security issue (for
example, something injectable through job payloads or key names), please
report it privately rather than opening a public issue:

https://github.com/HaulCo/pygowork/security/advisories/new

You will get a response as quickly as we can manage, and credit in the fix
unless you prefer otherwise.

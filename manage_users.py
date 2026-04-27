#!/usr/bin/env python3
"""User management for Scoutline. Run from the project root."""
import sqlite3, hashlib, secrets, sys, os, getpass

AUTH_DB = os.path.join('data', 'users.db')
PW_ITERATIONS = 100_000

def _hash_pw(password, salt, iterations=PW_ITERATIONS):
    return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), iterations).hex()

def create_user(username, password):
    os.makedirs('data', exist_ok=True)
    salt = secrets.token_hex(32)
    con = sqlite3.connect(AUTH_DB)
    try:
        con.execute('INSERT INTO users (username, pw_hash, salt, iterations) VALUES (?,?,?,?)',
                    (username, _hash_pw(password, salt), salt, PW_ITERATIONS))
        con.commit()
        print(f'  User "{username}" created.')
    except sqlite3.IntegrityError:
        print(f'  Error: user "{username}" already exists.')
    finally:
        con.close()

def delete_user(username):
    con = sqlite3.connect(AUTH_DB)
    cur = con.execute('DELETE FROM users WHERE username=?', (username,))
    con.commit(); con.close()
    if cur.rowcount:
        print(f'  User "{username}" deleted.')
    else:
        print(f'  User "{username}" not found.')

def list_users():
    con = sqlite3.connect(AUTH_DB)
    rows = con.execute('SELECT username, created FROM users ORDER BY created').fetchall()
    con.close()
    if not rows:
        print('  No users.')
    for u, c in rows:
        print(f'  {u}  (created {c})')

def change_password(username, password):
    salt = secrets.token_hex(32)
    con = sqlite3.connect(AUTH_DB)
    cur = con.execute('UPDATE users SET pw_hash=?, salt=?, iterations=? WHERE username=?',
                      (_hash_pw(password, salt), salt, PW_ITERATIONS, username))
    con.commit(); con.close()
    if cur.rowcount:
        print(f'  Password updated for "{username}".')
    else:
        print(f'  User "{username}" not found.')

HELP = '''Usage:
  python3 manage_users.py create <username>
  python3 manage_users.py delete <username>
  python3 manage_users.py passwd <username>
  python3 manage_users.py list
'''

if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'list':
        list_users()
    elif cmd == 'create':
        if len(sys.argv) < 3: print('Usage: create <username>'); sys.exit(1)
        pw = getpass.getpass('Password: ')
        pw2 = getpass.getpass('Confirm:  ')
        if pw != pw2: print('Passwords do not match.'); sys.exit(1)
        create_user(sys.argv[2], pw)
    elif cmd == 'delete':
        if len(sys.argv) < 3: print('Usage: delete <username>'); sys.exit(1)
        delete_user(sys.argv[2])
    elif cmd == 'passwd':
        if len(sys.argv) < 3: print('Usage: passwd <username>'); sys.exit(1)
        pw = getpass.getpass('New password: ')
        pw2 = getpass.getpass('Confirm:      ')
        if pw != pw2: print('Passwords do not match.'); sys.exit(1)
        change_password(sys.argv[2], pw)
    else:
        print(HELP)

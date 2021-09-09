"""
This tutorial will show you how to connect to the IBM i system and complete
basic functions using the Expression Language sqlalchmy method. For additional
functions see the tutorial in the docs here [1].
[1]: https://docs.sqlalchemy.org/en/13/core/tutorial.html
"""

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import select
from sqlalchemy.sql import and_, or_

system = input("System: ")
UID = input("UID: ")
PWD = input("PWD: ")
extra = input("Add opts: ")

conn_string = "ibmi://{}:{}@{}/?{}".format(UID, PWD, system, extra)

engine = create_engine(conn_string, echo=True)

# Creating Tables
print("Creating Tables")
metadata = MetaData()
users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(50)),
              Column('fullname', String(50)),
              )

addresses = Table('addresses', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('user_id', None, ForeignKey('users.id')),
                  Column('email_address', String(50), nullable=False),
                  )

metadata.create_all(engine)

# Insertions
print("Insertions")
with engine.connect() as conn:
    # single
    ins = users.insert().values(name='jack', fullname='Jack Jones')
    conn.execute(ins)

    # multiple
    conn.execute(addresses.insert(), [
           {'user_id': 1, 'email_address': 'jack@yahoo.com'},
           {'user_id': 1, 'email_address': 'jack@msn.com'},
        ])

    # Select statements
    print("Select statements")
    result = conn.execute(select([users]))

    for row in result:
        print(row)

    # Select specific columns
    result = conn.execute(select([users.c.name, users.c.fullname]))

    for row in result:
        print(row)

    # Conjunctions
    print("Conjunctions")
    s = select([(users.c.fullname +
                ", " + addresses.c.email_address).
                label('title')]).\
        where(
              and_(
                  users.c.id == addresses.c.user_id,
                  users.c.name.between('a', 'z'),
                  or_(
                     addresses.c.email_address.like('%@aol.com'),
                     addresses.c.email_address.like('%@msn.com')
                  )
              )
           )

    print(conn.execute(s).fetchall())

    # Textual SQL
    print("Textual SQL")
    s = "SELECT users.fullname || ', ' || addresses.email_address AS" \
        " title FROM users, addresses WHERE users.id = " \
        "addresses.user_id AND users.name BETWEEN ? AND ? " \
        "AND (addresses.email_address LIKE ? " \
        "OR addresses.email_address LIKE ?)" \

    print(conn.execute(s, 'm', 'z', '%@aol.com', '%@msn.com').fetchall())

    # Updates
    print("Updates")
    stmt = users.update(). where(users.c.name == 'jack').values(name='ed')
    conn.execute(stmt)

    result = conn.execute(select([users]))

    for row in result:
        print(row)

    # Deletion
    print("Deletions")
    conn.execute(users.delete().where(users.c.name > 'a'))

    result = conn.execute(select([users]))

    for row in result:
        print(row)

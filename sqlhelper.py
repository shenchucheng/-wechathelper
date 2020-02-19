from collections import Iterable
from logging import getLogger
logger = getLogger('sql')


# _rowid_
class SqliteHelper:
    show_tables = '''SELECT name FROM sqlite_master WHERE type="table" ORDER BY name'''

    @staticmethod
    def select(table_name, column='', condition='', limit='', order='',  **kwargs):
        """
        :param table_name: str like "sqlite_master"
        :param column: the column_name you want to query; str: name or
                        iterable tuple/list/set and element is str
                        Do not need "where"
        :param condition: str like "type=table";
                          dict like {"type": "table"};
                          ["type"] or ("type",) will generate type=?
        :param limit: int or int string
        :param order: order by column :str column name
        :param kwargs:
                     logic: and/or apply in condition's logic; default: and
                     desc: Boolean missing is ASC True is DESC
        :return: sql
        """
        if column:
            column = column if isinstance(column, str) else ', '.join(column)
        else:
            column = '*'
        if condition:
            if isinstance(condition, str):
                condition = condition.strip()
                if condition.lower().startswith('where '):
                    condition = condition.split('where ', 1)[-1].split()
            elif isinstance(condition, Iterable):
                join = ' ' + kwargs.get('logic', 'and').upper() + ' '
                if isinstance(condition, dict):
                    condition = join.join(('='.join((i, '"{}"'.format(v) if isinstance(v, str) else str(v)))
                                           for i, v in condition.items()))
                else:
                    condition = join.join('='.join((i, '?')) for i in condition)
            else:
                raise TypeError('Arg Condition Must str or Iterable')
            condition = ' WHERE ' + condition
        else:
            condition = ''
        if limit:
            limit = ' LIMIT {}'.format(int(limit))
        else:
            limit = ''
        if order:
            order = ' ORDER BY ' + (order.strip() if isinstance(order, str) else ', '.join(order))
            if kwargs.get('desc'):
                order += ' DESC'
        else:
            order = ''
        _sql = '''SELECT {} FROM {}'''.format(column, table_name) + condition + order + limit
        logger.info(_sql)
        return _sql

    @staticmethod
    def create_table(table_name, column, **kwargs):
        """
        :param table_name: str
        :param column: str: "id INT AUTOINCREMENT PRIMARY KEY, UserName CHAR"
                       dict like {"id": "INT AUTOINCREMENT PRIMARY KEY", "UserName": "CHAR" }
                       Iterable like ("id INT AUTOINCREMENT PRIMARY KEY", "UserName CHAR")
        :param kwargs:
                      unique : Iterable like ["id", "UserName"]
        :return:
        """
        if isinstance(column, str):
            column = column.strip()
        elif isinstance(column, dict):
            column = ', '.join(' '.join(i) for i in column.items())
        else:
            column = ', '.join(column)
        if kwargs:
            primary_key = kwargs.get('primary_key')
            unique = kwargs.get('unique')
            check = kwargs.get('check')
            other = ''
            if primary_key:
                other += ''', PRIMARY KEY ({})'''.format(
                    primary_key.strip() if isinstance(primary_key, str) else ', '.join(primary_key))
            if unique:
                other += ''', UNIQUE ({})'''.format(
                    unique.strip() if isinstance(unique, str) else ', '.join(unique))
            if check:
                other += ''', CHECK ({})'''.format(
                    check.strip() if isinstance(check, str) else ' and '.join(check))
        else:
            other = ''
        _sql = '''CREATE TABLE {} ({})'''.format(table_name, column + other)
        logger.info(_sql)
        return _sql

    @staticmethod
    def insert(table_name, column):
        """
        :param table_name:
        :param column:
        :return:
        """
        if isinstance(column, int):
            values = ', '.join('?'*column)
            _sql = '''INSERT INTO {} VALUES ({})'''.format(table_name, values)
        else:
            if isinstance(column, str):
                column = column.strip()
                values = len(column.split(','))
            else:
                values = len(column)
                column = ', '.join(_ for _ in column)
            _sql = '''INSERT INTO {} ({}) VALUES ({})'''.format(
                table_name, column, ", ".join('?'*values))
        logger.info(_sql)
        return _sql

    @staticmethod
    def update(table_name, column, condition, **kwargs):
        """
        :param table_name:
        :param column:
        :param condition:
        :param kwargs:
        :return:
        """
        if isinstance(column, str):
            pass
        elif isinstance(column, dict):
            column = ', '.join(' = '.join((i, v)) for i, v in column.items())
        elif isinstance(column, (tuple, list)):
            column = ', '.join(' = '.join((i, "?")) for i in column)
        else:
            raise TypeError()
        if isinstance(condition, str):
            condition = condition.strip()
            if condition.lower().startswith('where '):
                condition = condition.split('where ', 1)[-1].split()
        elif isinstance(condition, Iterable):
            join = ' ' + kwargs.get('logic', 'and').upper() + ' '
            if isinstance(condition, dict):
                condition = join.join(('='.join((i, '"{}"'.format(v) if isinstance(v, str) else str(v)))
                                       for i, v in condition.items()))
            else:
                condition = join.join('='.join((i, '?')) for i in condition)
        else:
            raise TypeError('Arg Condition Must str or Iterable')
        _sql = "UPDATE {} SET {} WHERE {}".format(table_name, column, condition)
        logger.info(_sql)
        return _sql


sqlitehelper = SqliteHelper()
#
# if __name__ == '__main__':
#     import logging
#     import sqlite3
#     s = _s = Sql
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     logger = logging.getLogger('SQL')
#     db = sqlite3.connect('../data/test.db')
#     try:
#
#         cursor = db.cursor()
#         cursor.execute(s.show_tables)
#         if not cursor.fetchall():
#             columns_ = ', '.join("column"+str(i) for i in range(1, 9))
#             for i in range(1, 15):
#                 cursor.execute('CREATE TABLE table{} ({})'.format(i, columns_))
#                 cursor.executemany('INSERT INTO table{i} VALUES ({" ,".join("?"*8)})', ([_ for _ in range(8)] for __ in range(10)))
#         db.commit()
#         for column_ in ["name", ("name",), ["name"], ("name", "type"), {"type", "name"}]:
#             for _condition in ["type='table'", {"type":"table", "1": 1},  {"type": "table", "1": 2}]:
#                 for desc in (0, 1):
#                     for limit in (1, 5, 7, 9):
#                         for order in ('name', ('name',), ('Name', 'type')):
#                             cursor.execute(s.select('sqlite_master', column_, _condition, limit=limit, order=order, desc=desc))
#                             logger.info(str(cursor.fetchall()))
#
#     except:
#         db.close()
#         raise

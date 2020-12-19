__all__ = ['HiveTypeConversion']

import json
from pyjdbc.types import JdbcTypeConversion, jdbctype


class HiveTypeConversion(JdbcTypeConversion):

    @jdbctype(getter='getString', setter='setString', pytype=list)
    def ARRAY(self, value):
        return self.json_str(value)

    @jdbctype(getter='getString', setter='setString', pytype=dict)
    def STRUCT(self, value):
        return self.json_str(value)

    @jdbctype(getter='getString', setter='setString', pytype=dict)
    def MAP(self, value):
        return self.json_str(value)

    def json_str(self, value):
        """
        Hive does not support ResultSet.getArray() - instead you must decode arrays manually.
        the string returned is valid json

        see: https://github.com/apache/hive/blob/master/jdbc/src/java/org/apache/hive/jdbc/HiveBaseResultSet.java#L556
        """
        value = str(value)
        if not value.strip():
            return None

        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            raise ValueError('unable to decode Hive json string: {}\nColumn Value:\n{}'.format(e, value))
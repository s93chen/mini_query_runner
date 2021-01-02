from sys import exit
from os.path import getsize
from operator import attrgetter
from collections import namedtuple, defaultdict


class QueryRunner:
    """
    A mini query runner that implements JOIN, ORDERBY, COUNTBY,
    TAKE, FROM and SELECT. JOIN is implemented using sort merge join and
    hash join.
    """

    def __init__(self):
        self.data_loaded = dict()
        self.keywords = {"FROM", "SELECT", "TAKE", "ORDERBY", "COUNTBY", "JOIN"}

    def run_query(self, query_str):
        """
        Param:
            query_str: string type, input query to run
        Returns:
            query result in form of list of namedtuples
        """

        query_steps = self._parse_query(query_str)

        if not query_steps:
            return

        source_file = query_steps[0][1]
        rows = self._load_data(source_file)

        for i in range(1, len(query_steps)):

            if not rows:
                break

            action = query_steps[i][0]
            arg = query_steps[i][1]

            if action == "SELECT":
                rows = self._select(rows, arg)

            elif action == "TAKE":
                rows = self._take(rows, arg)

            elif action == "ORDERBY":
                rows = self._orderby(rows, arg, True)

            elif action == "COUNTBY":
                rows = self._countby(rows, arg)

            elif action == "JOIN":
                # rows = self._join(rows, arg, self._hash_join)
                rows = self._join(rows, arg, self._merge_join)

        return rows

    def print_output(self, output):
        """
        Prints query output in required form
        Param:
            output: list of namedtuples
        """
        try:
            if not output:
                print("No data returned")

            else:
                print(",".join(output[0]._fields))
                for row in output:
                    print(",".join(str(r) for r in row))

        except Exception:
            for i in output:
                print(i)

    def _select(self, rows, cols):
        """
        Param:
            rows: list of namedtuples as input data
            cols: list of strings as column names to select
        Returns:
            list of namedtuples with specified columns only
        """
        cols = cols.rstrip().split(",")

        for c in cols:
            if c not in rows[0]._fields:
                print(f"column {c} does not exist")
                return

        Row = namedtuple("Row", cols)
        return [Row(*[getattr(r, c) for c in cols]) for r in rows]

    def _take(self, rows, num_rows):
        """
        Param:
            rows: list of namedtuples as input data
            num_rows: integer as number of rows to return. If
                      negative, return last n rows.
        Returns:
            list of namedtuples with length of num_rows
        """
        if num_rows < 0:
            return rows[num_rows:]

        return rows[:num_rows]

    def _orderby(self, rows, order_col, reverse):
        """
        Param:
            rows: list of namedtuples as input data
            order_col: string, sortby column
            reverse: boolean, if True, sort in descending order
        Returns:
            list of named tuples sorted by order_col
        """
        return sorted(rows, key=attrgetter(order_col), reverse=reverse)

    def _countby(self, rows, count_col):

        count_dict = defaultdict(int)
        Row = namedtuple("Row", [count_col, "count"])

        for r in rows:
            count_dict[getattr(r, count_col)] += 1

        return [Row(k, v) for k, v in count_dict.items()]

    def _join(self, left_rows, join_args, join_fcn):
        """
        Wrapper function for join implementation of choice.

        param:
            left_rows: list of named tuples as left table
            join_args: tuple of 2 strings (right_table_name, join_column)
            join_fcn: function object for joining
        returns:
            list of namedtuples
        """
        right_rows = self._load_data(join_args[0])
        join_col = join_args[1]

        return join_fcn(left_rows, right_rows, join_col)

    def _get_joined_cols(self, left_rows, right_rows, join_col):
        """
        Helper function for joining functions - get list
        of column names for the joining output.

        param:
            left_rows: list of namedtuples as left table
            right_rows: list of namedtuples as right table
            join_col: string, join column name
        returns:
            list of strings
        """
        left_cols = list(left_rows[0]._fields)
        right_cols = [c for c in right_rows[0]._fields if c != join_col]

        return left_cols + right_cols

    def _hash_join(self, left_rows, right_rows, join_col):
        """
        Reference:
            https://en.wikipedia.org/wiki/Hash_join#Classic_hash_join
        """

        # JoinRow will be used to construct namedtuples
        # for the join output, with expected column names.

        output_cols = self._get_joined_cols(left_rows, right_rows, join_col)
        JoinRow = namedtuple("Row", output_cols)

        # build phase:
        # use the smaller table to create a hash table, where
        # the keys are distinct values in join column, and the
        # values are lists of indices to matching rows in the
        # smaller table.

        lookup = defaultdict(list)

        if len(left_rows) <= len(right_rows):
            small = left_rows
            big = right_rows
        else:
            small = right_rows
            big = left_rows

        for i, row in enumerate(small):
            lookup[getattr(row, join_col)].append(i)

        # probe phase:
        # for each row in the bigger table, check if there
        # is a match in the lookup table. If there is, create
        # joined records.

        output = []
        for big_row in big:
            key = getattr(big_row, join_col)

            if key in lookup:
                for i in lookup[key]:

                    # create join record
                    join_val = small[i]._asdict()
                    join_val.update(big_row._asdict())
                    output.append(JoinRow(**join_val))

        return output

    def _merge_join(self, left_rows, right_rows, join_col):
        """
        Nice lecture on sort merge join:
            https://www.youtube.com/watch?v=jiWCPJtDE2c
        """

        output_cols = self._get_joined_cols(left_rows, right_rows, join_col)
        JoinRow = namedtuple("Row", output_cols)
        join_output = []

        # sort both data input by join key
        sorted_left = self._orderby(left_rows, join_col, False)
        sorted_right = self._orderby(right_rows, join_col, False)

        # merge phase
        # the cursors are used to
        # track positions in data input

        mark = None
        left_cur = 0
        right_cur = 0

        while (left_cur < len(sorted_left)) and (right_cur < len(sorted_right)):

            left_val = getattr(sorted_left[left_cur], join_col)
            right_val = getattr(sorted_right[right_cur], join_col)

            if not mark:

                # advance left cursor to where right
                # cursor if left join value is smaller
                # than right join value.

                while left_val < right_val:
                    left_cur += 1
                    left_val = getattr(sorted_left[left_cur], join_col)

                # similar to above
                while right_val < left_val:
                    right_cur += 1
                    right_val = getattr(sorted_right[right_cur], join_col)

                mark = right_cur

            # when there is a match, create join record
            # and increment right cursor.

            if left_val == right_val:
                join_data = sorted_left[left_cur]._asdict()
                join_data.update(sorted_right[right_cur]._asdict())
                join_output.append(JoinRow(**join_data))

                right_cur += 1

            # If there is no match, reset right cursor
            # and mark, and increment left cursor.

            else:
                right_cur = mark
                left_cur += 1
                mark = None

        return join_output

    def _parse_and_infer_schema(self, row):
        """
        param:
            row: string, a line of input data
        returns:
            row as a list of tokens. eval() executes the string
            literal enclosed, but here we use it to convert a
            number back to numerical type.
        """
        r = row.rstrip().split(",")
        return [eval(t) if t.isnumeric() else t for t in r]

    def _load_data(self, file_name):
        """
        Loads data given csv file name
        param:
            file_name: string
        returns:
            list of namedtuples
        """
        try:
            if file_name in self.data_loaded:
                data = self.data_loaded[file_name]

            else:
                if not getsize(file_name):
                    print("Empty file")
                    return

                with open(file_name, "r") as f:

                    # extracts header
                    columns = next(f).rstrip().split(",")
                    Row = namedtuple("Row", columns)

                    # reads rest of the file
                    data = [Row(*self._parse_and_infer_schema(row)) for row in f]

                if not data:
                    print("Empty file")
                    return

                self.data_loaded[file_name] = data

            return data

        except Exception as err:
            print(err.args[1])
            return

    def _parse_query(self, query_str):
        """
        Parse and validate input query string.
        Returns list of lists, where each list is in the form:
            [query_action, action_argument]
        """
        tokens = query_str.split()

        if not tokens:
            print("No query entered")
            return

        if tokens[0] != "FROM":
            print("Missing data source")
            return

        query_steps = []

        act_idx = 0
        arg_idx = 1

        while arg_idx < len(tokens):

            cur_action = tokens[act_idx]

            if cur_action not in self.keywords:
                print(f"Invalid input at {act_idx + 1}th token")
                return

            elif cur_action == "JOIN":
                arg_1 = tokens[arg_idx]
                arg_2 = tokens[arg_idx + 1]

                if (arg_1 in self.keywords) or (arg_2 in self.keywords):
                    print(f"Missing JOIN argument at {act_idx + 1}th token")
                    return

                if ".csv" not in arg_1:
                    print(f"Missing JOIN table at {act_idx + 1}th token")
                    return

                cur_arg = (arg_1, arg_2)
                act_idx = arg_idx + 2

            else:
                cur_arg = tokens[arg_idx]

                if cur_arg in self.keywords:
                    print(f"Missing {cur_action} argument")
                    return

                if cur_action == "TAKE":
                    if not cur_arg.isnumeric():
                        print("TAKE requires integer input")
                        return

                    cur_arg = int(cur_arg)

                act_idx = arg_idx + 1

            query_steps.append([cur_action, cur_arg])
            arg_idx = act_idx + 1

        return query_steps


if __name__ == "__main__":

    # FROM ./data/pokemon.csv JOIN ./data/stats.csv id JOIN ./data/legendary.csv id TAKE 10
    # FROM ./data/pokemon.csv COUNTBY type1 ORDERBY count TAKE 5
    # FROM ./data/pokemon.csv SELECT name,type1 TAKE 5

    qr = QueryRunner()

    while 1:
        try:
            query = input("> ")
            out = qr.run_query(query)
            qr.print_output(out)

        # exits nicely at ctrl-D (sends an End-of-File signal)
        # or at ctrl-C (sends terminate signal)

        except (EOFError, KeyboardInterrupt):
            print("")
            exit(0)

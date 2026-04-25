echo "This is my sql parsing library"
echo

ss src/*.c include/sql-parser-library/*.h
ss src/specs/*.c src/group_specs/*.c src/window_specs/*.c

echo
echo
echo "Here is the program which runs the whole thing."
echo
echo

ss examples/dbgen_test/src/main.c

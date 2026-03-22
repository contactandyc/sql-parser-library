# SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
# SPDX-License-Identifier: Apache-2.0

bin/cat_source.sh `ls include/*/sql_types.h src/sql_[bdfis]*.c src/sql_types.c` > sources.txt

cat sources.txt | pbcopy

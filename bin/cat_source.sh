# SPDX-FileCopyrightText: 2024–2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-FileCopyrightText: 2024–2025 Knode.ai
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

for arg in "$@"; do
    echo "# $arg:"
    echo
    cat "$arg"
    echo
    echo
    echo
    echo
done

# SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
# SPDX-License-Identifier: Apache-2.0

for arg in "$@"; do
    echo "# $arg:"
    echo
    cat "$arg"
    echo
    echo
    echo
    echo
done

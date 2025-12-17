package com.mcnealysoftware.readcommitted;

import java.util.Collection;

public record Page<T>(Collection<T> items, long total) {
}

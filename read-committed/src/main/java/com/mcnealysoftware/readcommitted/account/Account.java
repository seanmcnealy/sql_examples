package com.mcnealysoftware.readcommitted.account;

import java.math.BigDecimal;

public record Account(Long id, String name, BigDecimal balance) {
}

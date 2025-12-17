package com.mcnealysoftware.readcommitted.account;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.data.domain.Page;

import java.math.BigDecimal;

@Service
public class AccountRepositoryJPAService {

    @Autowired
    AccountRepositoryJPA accountRepository;

    @Transactional(readOnly = false, isolation = Isolation.READ_UNCOMMITTED)
    public long createAccount(String name, BigDecimal balance) {
        final var account = new Account();
        account.setName(name);
        account.setBalance(balance);
        accountRepository.save(account);
        return account.getId();
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_COMMITTED)
    public Page<Account> getAccounts(int page, int pageSize) {
        return accountRepository.findAll(PageRequest.of(page, pageSize, Sort.by("id").descending()));
    }

    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    public Page<Account> getAccountsRepeatableRead(int page, int pageSize) {
        return accountRepository.findAll(PageRequest.of(page, pageSize));
    }
}

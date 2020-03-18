package com.example.csvsender.dtos;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import lombok.Data;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Data
@JsonRootName("invoice")
public class InvoiceDTO {
    @JsonAlias(value = "invoice_date")
    @JsonProperty(value = "dateCreated", required = true)
    private LocalDate invoiceDate;
    @JsonAlias(value = "invoice_due_date")
    @JsonProperty(value = "dueDate", required = true)
    private LocalDate dueDate;
    @JsonProperty(value = "status", required = true)//?
    private String status;
    @JsonProperty(value = "currency", required = true)
    private String currency;
    @JsonAlias(value = "exchange_rate")
    @JsonProperty(value = "exchangeRate", required = true)
    private Double exchangeRate;
    @JsonProperty(value = "discount", defaultValue = "0")
    private Double discount;
    @JsonAlias(value = "tax")
    @JsonProperty(value = "taxTotal", required = true)
    private Double tax;
    @JsonProperty(value = "paid", required = true)
    private Double paid;
    @JsonProperty(value = "total", required = true)
    private Double total;
    @JsonAlias(value = "internal_id")
    @JsonProperty(value = "invoiceId", required = true)
    private String internalId;
    @JsonAlias(value = "invoice_number")
    @JsonProperty(value = "invoiceNumber", required = true)
    private String invoiceNumber;
    @JsonProperty(value = "deleted", defaultValue = "false")
    private boolean deleted;
    @JsonAlias(value = "customer_id")
    @JsonProperty(value = "customerId", required = true)
    private String customerId;
    @JsonProperty(value = "custom_fields")
    private Map<String, String> customFields;
    @JsonProperty(value = "close_date")
    private LocalDate closeDate;
    @JsonProperty(value = "terms")
    private String terms;
    @JsonProperty(value = "invoice_lines")
    private List<InvoiceLineDTO> invoiceLines;
}

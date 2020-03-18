package com.example.csvsender.dtos;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

@Data
public class InvoiceLineDTO {
    @JsonAlias()
    @JsonProperty(value = "description")
    private String description;
    @JsonProperty(value = "quantity", required = true)
    private Double quantity;
    @JsonProperty(value = "rate", required = true)
    private Double rate;
    @JsonProperty(value = "amount", required = true)
    private Double amount;
    @JsonProperty(value = "invoiceId", required = true)
    private String invoiceId;
    @JsonProperty(value = "name", required = true)
    private String name;
    @JsonProperty(value = "custom_fields")
    private Map<String, String> customFields;
}

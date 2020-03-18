package com.example.csvsender.rest;

import com.example.csvsender.services.CSVReaderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Controller(value = "/")
public class CSVReaderRestController {
    @Autowired
    private CSVReaderService service;
    @Autowired
    private ObjectMapper mapper;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .build();

    @GetMapping("/pushdata")
    public void getAll() throws IOException, InterruptedException {
        String json = mapper.writeValueAsString(service.getInvoicesFromCSV());
                HttpRequest request = HttpRequest.newBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .uri(URI.create("http://localhost:8080/bulk/invoices"))
                    .setHeader("User-Agent", "Java 11 HttpClient Bot") // add request header
                    .header("Content-Type", "application/json")
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.statusCode());
            System.out.println(response.body());
    }
}

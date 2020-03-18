package com.example.csvsender.services;

import com.example.csvsender.dtos.InvoiceDTO;
import com.example.csvsender.dtos.InvoiceLineDTO;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Service
public class CSVReaderService {
    private final String INVOICE_FILE_PATH = "invoice.csv";
    private final String INVOICE_LINES_FILE_PATH = "invoiceLines.csv";
    private List<InvoiceDTO> cache;
    private Map<String, List<InvoiceLineDTO>> lineDTOS;
    CsvMapper csvMapper;

    @PostConstruct
    private void postConstructSetup() {
        csvMapper = new CsvMapper();
        csvMapper.registerModule(new JavaTimeModule());
        csvMapper.registerModule(new TrimModule());
        csvMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }


    public List<InvoiceDTO> getInvoicesFromCSV() {
        if (cache == null) {
            long start = System.nanoTime();
            List<InvoiceDTO> invoiceList = processCSV(INVOICE_FILE_PATH);
            Map<String, List<InvoiceLineDTO>> invoiceLineMap = processNestedCSV(INVOICE_LINES_FILE_PATH);
            invoiceList.stream().forEach(i -> {
                i.setInvoiceLines(invoiceLineMap.get(i.getInternalId()));
            });
            cache = invoiceList;
            long finish = System.nanoTime();
            System.out.printf("Process took %d second\n", (finish - start)/1000000000);
            System.out.println("InvoiceList size is: " + cache.size());
        }
        return cache;
    }

    private Map<String, List<InvoiceLineDTO>> processNestedCSV(String path) {
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        ObjectReader oReader = csvMapper.readerFor(InvoiceLineDTO.class).with(schema);
        Map<String, List<InvoiceLineDTO>> result = new HashMap<>();
        try (Reader reader = Files.newBufferedReader(
                Paths.get(
                        ClassLoader.getSystemResource(path).toURI()))) {
            MappingIterator<InvoiceLineDTO> mi = oReader.readValues(reader);
            while (mi.hasNext()) {
                InvoiceLineDTO current = mi.next();
                List<InvoiceLineDTO> list = result.get(current.getInvoiceId());
                if (list == null) {
                    list = new LinkedList<>();
                    result.put(current.getInvoiceId(), list);
                }
                list.add(current);
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        lineDTOS = result;
        return result;
    }

    private List<InvoiceDTO> processCSV(String path) {
        CsvSchema schema = CsvSchema.emptySchema().withHeader();
        ObjectReader oReader = csvMapper.readerFor(InvoiceDTO.class).with(schema);
        List<InvoiceDTO> invoiceDTOS = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(
                Paths.get(
                        ClassLoader.getSystemResource(path).toURI()))) {
            MappingIterator<InvoiceDTO> mi = oReader.readValues(reader);
            while (mi.hasNext()) {
                InvoiceDTO current = mi.next();
                invoiceDTOS.add(current);
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return invoiceDTOS;
    }
}

class TrimModule extends SimpleModule {

    public TrimModule() {
        addDeserializer(String.class, new StdScalarDeserializer<String>(String.class) {
            @Override
            public String deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
                return jp.getValueAsString().trim();
            }
        });
    }
}
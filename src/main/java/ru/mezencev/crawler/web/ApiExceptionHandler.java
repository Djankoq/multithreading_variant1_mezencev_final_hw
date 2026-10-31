package ru.mezencev.crawler.web;

import java.util.stream.Collectors;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ProblemDetail> validation(MethodArgumentNotValidException e) {
        return problem(HttpStatus.BAD_REQUEST, e.getBindingResult().getFieldErrors().stream()
                .map(error -> error.getField() + ": " + error.getDefaultMessage()).collect(Collectors.joining("; ")));
    }
    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentTypeMismatchException.class, HttpMessageNotReadableException.class})
    ResponseEntity<ProblemDetail> badRequest(Exception e) {
        return problem(HttpStatus.BAD_REQUEST, e instanceof IllegalArgumentException ? e.getMessage() : "Некорректные параметры или JSON запроса");
    }
    @ExceptionHandler(ResponseStatusException.class)
    ResponseEntity<ProblemDetail> status(ResponseStatusException e) { return problem(e.getStatusCode(), e.getReason()); }
    private ResponseEntity<ProblemDetail> problem(HttpStatusCode status, String detail) {
        return ResponseEntity.status(status).body(ProblemDetail.forStatusAndDetail(status, detail == null ? "Ошибка запроса" : detail));
    }
}

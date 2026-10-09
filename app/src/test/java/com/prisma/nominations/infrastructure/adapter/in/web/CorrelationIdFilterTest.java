package com.prisma.nominations.infrastructure.adapter.in.web;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("E1")
class CorrelationIdFilterTest {

    private final CorrelationIdFilter filter = new CorrelationIdFilter();

    @Test
    void propagatesValidHeaderToMdcAndResponse() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/v1/nominations");
        request.addHeader(ApiHeaders.CORRELATION_ID, "abc-123_X.9");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> seenInMdc = new AtomicReference<>();

        filter.doFilter(request, response, (req, res) -> seenInMdc.set(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)));

        assertThat(seenInMdc.get()).isEqualTo("abc-123_X.9");
        assertThat(response.getHeader(ApiHeaders.CORRELATION_ID)).isEqualTo("abc-123_X.9");
        assertThat(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)).isNull();
    }

    @Test
    void generatesUuidWhenHeaderIsMissing() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> seenInMdc = new AtomicReference<>();

        filter.doFilter(new MockHttpServletRequest("GET", "/"), response,
                (req, res) -> seenInMdc.set(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)));

        String generated = response.getHeader(ApiHeaders.CORRELATION_ID);
        assertThat(UUID.fromString(generated)).isNotNull();
        assertThat(seenInMdc.get()).isEqualTo(generated);
        assertThat(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc\nINFO fake log line", "con espacios", "", "valor;inyectado", "4111111111111111<script>"})
    void replacesInvalidHeaderWithGeneratedUuid(String invalid) throws Exception {
        assertReplaced(invalid);
    }

    @Test
    void replacesTooLongHeader() throws Exception {
        assertReplaced("a".repeat(200));
        assertThat(CorrelationIdFilter.resolve("a".repeat(64))).isEqualTo("a".repeat(64));
    }

    @Test
    void cleansMdcEvenWhenChainThrows() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/");
        request.addHeader(ApiHeaders.CORRELATION_ID, "corr-1");

        assertThatThrownBy(() -> filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)).isNull();
    }

    private void assertReplaced(String invalid) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/");
        request.addHeader(ApiHeaders.CORRELATION_ID, invalid);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (req, res) -> { });

        String returned = response.getHeader(ApiHeaders.CORRELATION_ID);
        assertThat(returned).isNotEqualTo(invalid);
        assertThat(UUID.fromString(returned)).isNotNull();
        assertThat(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)).isNull();
    }
}

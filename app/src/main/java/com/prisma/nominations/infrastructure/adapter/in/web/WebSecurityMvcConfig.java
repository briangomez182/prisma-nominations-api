package com.prisma.nominations.infrastructure.adapter.in.web;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/** Registra el resolver de {@link AuthenticatedEntity} (también lo levanta un {@code @WebMvcTest}). */
@Configuration(proxyBeanMethods = false)
class WebSecurityMvcConfig implements WebMvcConfigurer {

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(new AuthenticatedEntityArgumentResolver());
    }
}

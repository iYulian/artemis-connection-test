package org.example.artemisconnectiontest.vtbartemis.utils;

import jakarta.xml.bind.annotation.*;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.time.LocalDateTime;

@SuperBuilder
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder =
        "additionalData"
)
public class Test {
    @XmlElementWrapper(name = "AdditionalData")
    @XmlElement(name = "DataItem", namespace = "urn:common.service.ti.apps.tiplus2.misys.com")
    protected LocalDateTime additionalData;

    public LocalDateTime getAdditionalData() {
        return additionalData;
    }

    public void setAdditionalData(LocalDateTime additionalData) {
        this.additionalData = additionalData;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    @Override
    public boolean equals(Object that) {
        return EqualsBuilder.reflectionEquals(this, that);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    public static void main(String[] args) {
        Test test = Test.builder().additionalData(LocalDateTime.now()).build();
        System.out.println(test);
    }
}

/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   May 24, 2020 (dietzc): created
 */
package org.knime.core.data.container.table.legacy;

import org.knime.core.data.table.store.ColumnChunk;
import org.knime.core.data.table.store.ColumnChunkSpec;
import org.knime.core.data.table.store.domain.Domain;
import org.knime.core.data.table.store.domain.DomainCalculationConfig;
import org.knime.core.data.table.store.domain.DomainCalculator;
import org.knime.core.data.table.store.domain.NumericDomain;
import org.knime.core.data.table.store.domain.NumericDomain.NumericDomainCalculator;
import org.knime.core.data.table.store.domain.StringDomain;
import org.knime.core.data.table.store.domain.StringDomain.StringDomainCalculator;
import org.knime.core.data.table.store.types.BinarySupplChunk;
import org.knime.core.data.table.store.types.BinarySupplChunk.BinarySupplChunkSpec;
import org.knime.core.data.table.store.types.NumericReadColumnChunk;
import org.knime.core.data.table.store.types.StringChunk;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz
 */
public class LegacyDomainConfig implements DomainCalculationConfig {
    @Override
    public <C extends ColumnChunk, D extends Domain> DomainCalculator<C, D> getCalculator(final int index,
        final ColumnChunkSpec<C> spec) {
        if (spec instanceof BinarySupplChunkSpec) {
            Class<?> chunkType = ((BinarySupplChunkSpec<?>)spec).getChildSpec().getChunkType();
            if (NumericReadColumnChunk.class.isAssignableFrom(chunkType)) {
                @SuppressWarnings("unchecked")
                final DomainCalculator<C, D> calc =
                    (DomainCalculator<C, D>)new LegacyDomainCalculator<NumericReadColumnChunk, NumericDomain>(
                        new NumericDomainCalculator<>());
                return calc;
            } else if (chunkType == StringChunk.class) {
                @SuppressWarnings("unchecked")
                final DomainCalculator<C, D> calc =
                    (DomainCalculator<C, D>)new LegacyDomainCalculator<StringChunk, StringDomain>(
                        new StringDomainCalculator(/* TODO CONFIGURABLE */120));
                return calc;
            }
        }
        return null;
    }

    class LegacyDomainCalculator<C extends ColumnChunk, D extends Domain>
        implements DomainCalculator<BinarySupplChunk<C>, D> {

        private DomainCalculator<C, D> m_delegate;

        LegacyDomainCalculator(final DomainCalculator<C, D> delegate) {
            m_delegate = delegate;
        }

        @Override
        public D createEmpty() {
            return m_delegate.createEmpty();
        }

        @Override
        public D merge(final D result, final D stored) {
            return m_delegate.merge(result, stored);
        }

        @Override
        public D apply(final BinarySupplChunk<C> t) {
            return m_delegate.apply(t.getChunk());
        }
    }
}

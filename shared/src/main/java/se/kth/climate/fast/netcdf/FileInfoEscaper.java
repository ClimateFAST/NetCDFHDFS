/*
 * Copyright (C) 2017 KTH Royal Institute of Technology
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.climate.fast.netcdf;

import com.google.common.escape.CharEscaper;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import se.kth.climate.fast.FASTConstants;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class FileInfoEscaper {

    private static final CustomEscaper escaper = new CustomEscaper();

    public static FileInfoEscaper INSTANCE = new FileInfoEscaper();

    private FileInfoEscaper() {
    }

    public String escape(String string) {
        try {
            String urlResult = URLEncoder.encode(string, FASTConstants.ENC_SCHEME);
            return escaper.escape(urlResult);
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex); // can hardly recover from this
        }
    }

    public String unescape(String string) {
        try {
            String customResult = escaper.unescape(string);
            String urlResult = URLDecoder.decode(customResult, FASTConstants.ENC_SCHEME);
            return urlResult;
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex); // can hardly recover from this
        }
    }

    public static final class CustomEscaper extends CharEscaper {

        private static final char[] US_ESCAPE_SEQ = new char[]{'%', '5', 'F'}; // _ in unicode hex
        private static final String US_NEEDLE = new String(US_ESCAPE_SEQ);
        private static final char[] TILDE_ESCAPE_SEQ = new char[]{'%', '7', 'E'}; // ~ in unicode hex
        private static final String TILDE_NEEDLE = new String(TILDE_ESCAPE_SEQ);

        @Override
        protected char[] escape(char c) {
            switch (c) {
                case '_':
                    return US_ESCAPE_SEQ;
                case '~':
                    return TILDE_ESCAPE_SEQ;
                default:
                    return null;
            }
        }

        public String unescape(String s) {
            String s1 = s.replaceAll(US_NEEDLE, "_");
            String s2 = s1.replaceAll(TILDE_NEEDLE, "~");
            return s2;
        }

    }

}

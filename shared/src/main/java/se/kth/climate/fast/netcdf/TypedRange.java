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

import com.google.common.base.Converter;
import com.google.common.base.Optional;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import org.javatuples.Pair;
import static se.kth.climate.fast.FASTConstants.ENC_SCHEME;
import ucar.ma2.Array;
import ucar.ma2.DataType;

/**
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public abstract class TypedRange {

    static final String SEP = "~";

    protected final DataType type;

    protected TypedRange(DataType type) {
        this.type = type;
    }

    public DataType getDataType() {
        return this.type;
    }

    abstract public Pair<Double, Double> getDouble();

    abstract public Pair<Float, Float> getFloat();

    abstract public Pair<Long, Long> getLong();

    abstract public Pair<Integer, Integer> getInt();

    abstract public Pair<Short, Short> getShort();

    abstract public Pair<Byte, Byte> getByte();

    abstract public Pair<Character, Character> getChar();

    abstract public Pair<Boolean, Boolean> getBoolean();

    public Pair<String, String> getString() {
        if (type.isString()) {
            Pair p = getObject();
            Pair<String, String> pS = p; // We know already the Objects are going to be Strings, so we can just cheat the compiler like this
            return p;
        } else {
            Pair<Object, Object> p = getObject();
            return Pair.with(p.getValue0().toString(), p.getValue1().toString());
        }
    }

    abstract public Pair<Object, Object> getObject();

    abstract public void appendToString(StringBuilder sb);

    public static Optional<TypedRange> fromString(String in) {
        String[] inParts = in.split(SEP);
        if (inParts.length != 3) {
            throw new IllegalArgumentException("Input String \"" + in + "\" does not have the required format TYPE:START:END!");
        }
        DataType type = DataType.getType(inParts[0]);
        if (type == null) {
            throw new IllegalArgumentException("Can't find valid type for " + inParts[0]);
        }
        switch (type) {
            case FLOAT:
                return FPTypedRange.floatFromString(inParts[1], inParts[2]);
            case DOUBLE:
                return FPTypedRange.doubleFromString(inParts[1], inParts[2]);
            case LONG:
                return IntTypedRange.longFromString(inParts[1], inParts[2]);
            case INT:
                return IntTypedRange.intFromString(inParts[1], inParts[2]);
            case SHORT:
                return IntTypedRange.shortFromString(inParts[1], inParts[2]);
            case BYTE:
                return IntTypedRange.byteFromString(inParts[1], inParts[2]);
            case CHAR:
                return CharTypedRange.charFromString(inParts[1], inParts[2]);
            case STRING:
                return CharTypedRange.stringFromString(inParts[1], inParts[2]);
            case BOOLEAN:
                return BoolTypedRange.booleanFromString(inParts[1], inParts[2]);
            default:
                return Optional.absent();
        }
    }

    public static TypedRange with(float start, float end) {
        return new FPTypedRange(start, end);
    }

    public static TypedRange with(double start, double end) {
        return new FPTypedRange(start, end);
    }

    public static TypedRange with(long start, long end) {
        return new IntTypedRange(start, end);
    }

    public static TypedRange with(int start, int end) {
        return new IntTypedRange(start, end);
    }

    public static TypedRange with(short start, short end) {
        return new IntTypedRange(start, end);
    }

    public static TypedRange with(byte start, byte end) {
        return new IntTypedRange(start, end);
    }

    public static TypedRange with(char start, char end) {
        return new CharTypedRange(start, end);
    }

    public static TypedRange with(boolean start, boolean end) {
        return new BoolTypedRange(start, end);
    }

    public static Optional<TypedRange> fromArray(Array data, int start, int end) {
        switch (data.getDataType()) {
            case FLOAT:
                return Optional.of(TypedRange.with(data.getFloat(start), data.getFloat(end)));
            case DOUBLE:
                return Optional.of(TypedRange.with(data.getDouble(start), data.getDouble(end)));
            case LONG:
                return Optional.of(TypedRange.with(data.getLong(start), data.getLong(end)));
            case INT:
                return Optional.of(TypedRange.with(data.getInt(start), data.getInt(end)));
            case SHORT:
                return Optional.of(TypedRange.with(data.getShort(start), data.getShort(end)));
            case BYTE:
                return Optional.of(TypedRange.with(data.getByte(start), data.getByte(end)));
            case CHAR:
                return Optional.of(TypedRange.with(data.getChar(start), data.getChar(end)));
            case BOOLEAN:
                return Optional.of(TypedRange.with(data.getBoolean(start), data.getBoolean(end)));
            default:
                return Optional.absent();
        }
    }

    private static class FPTypedRange extends TypedRange {

        private final static Converter<String, Float> CONVF = Floats.stringConverter();
        private final static Converter<String, Double> CONVD = Doubles.stringConverter();
        private final double start;
        private final double end;

        private FPTypedRange(float start, float end) {
            super(DataType.FLOAT);
            this.start = start;
            this.end = end;
        }

        private FPTypedRange(double start, double end) {
            super(DataType.DOUBLE);
            this.start = start;
            this.end = end;
        }

        @Override
        public Pair<Double, Double> getDouble() {
            return Pair.with(start, end);
        }

        @Override
        public Pair<Float, Float> getFloat() {
            return Pair.with((float) start, (float) end);
        }

        @Override
        public Pair<Long, Long> getLong() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Integer, Integer> getInt() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Short, Short> getShort() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Byte, Byte> getByte() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Character, Character> getChar() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Boolean, Boolean> getBoolean() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Object, Object> getObject() {
            return Pair.with((Object) start, (Object) end);
        }

        @Override
        public void appendToString(StringBuilder sb) {
            try {
                String startS;
                String endS;
                if (type == DataType.FLOAT) {
                    startS = CONVF.reverse().convert((float) start);
                    endS = CONVF.reverse().convert((float) end);
                } else if (type == DataType.DOUBLE) {
                    startS = CONVD.reverse().convert(start);
                    endS = CONVD.reverse().convert(end);
                } else {
                    throw new RuntimeException("How the fuck did we get here?");
                }
                String startSE = URLEncoder.encode(startS, ENC_SCHEME);
                String endSE = URLEncoder.encode(endS, ENC_SCHEME);
                sb.append(type.name());
                sb.append(SEP);
                sb.append(startSE);
                sb.append(SEP);
                sb.append(endSE);
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

        private static Optional<TypedRange> floatFromString(String startS, String endS) {
            try {
                String startSD = URLDecoder.decode(startS, ENC_SCHEME);
                String endSD = URLDecoder.decode(endS, ENC_SCHEME);
                float start = CONVF.convert(startSD);
                float end = CONVF.convert(endSD);
                return Optional.of(new FPTypedRange(start, end));
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

        private static Optional<TypedRange> doubleFromString(String startS, String endS) {
            try {
                String startSD = URLDecoder.decode(startS, ENC_SCHEME);
                String endSD = URLDecoder.decode(endS, ENC_SCHEME);
                double start = CONVD.convert(startSD);
                double end = CONVD.convert(endSD);
                return Optional.of(new FPTypedRange(start, end));
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

    }

    private static class IntTypedRange extends TypedRange {

        private final static Converter<String, Long> CONVL = Longs.stringConverter();
        private final long start;
        private final long end;

        private IntTypedRange(int start, int end) {
            super(DataType.INT);
            this.start = start;
            this.end = end;
        }

        private IntTypedRange(long start, long end) {
            super(DataType.LONG);
            this.start = start;
            this.end = end;
        }

        private IntTypedRange(short start, short end) {
            super(DataType.SHORT);
            this.start = start;
            this.end = end;
        }

        private IntTypedRange(byte start, byte end) {
            super(DataType.BYTE);
            this.start = start;
            this.end = end;
        }

        @Override
        public Pair<Double, Double> getDouble() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Float, Float> getFloat() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Long, Long> getLong() {
            return Pair.with(start, end);
        }

        @Override
        public Pair<Integer, Integer> getInt() {
            return Pair.with((int) start, (int) end);
        }

        @Override
        public Pair<Short, Short> getShort() {
            return Pair.with((short) start, (short) end);
        }

        @Override
        public Pair<Byte, Byte> getByte() {
            return Pair.with((byte) start, (byte) end);
        }

        @Override
        public Pair<Character, Character> getChar() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Boolean, Boolean> getBoolean() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Object, Object> getObject() {
            return Pair.with((Object) start, (Object) end);
        }

        @Override
        public void appendToString(StringBuilder sb) {
            try {
                String startS = CONVL.reverse().convert(start);
                String endS = CONVL.reverse().convert(end);
                String startSE = URLEncoder.encode(startS, ENC_SCHEME);
                String endSE = URLEncoder.encode(endS, ENC_SCHEME);
                sb.append(type.name());
                sb.append(SEP);
                sb.append(startSE);
                sb.append(SEP);
                sb.append(endSE);
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

        private static long readLong(String s) {
            try {
                String sD = URLDecoder.decode(s, ENC_SCHEME);
                return CONVL.convert(sD);
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

        private static Optional<TypedRange> longFromString(String startS, String endS) {
            long start = readLong(startS);
            long end = readLong(endS);
            return Optional.of(new IntTypedRange(start, end));
        }

        private static Optional<TypedRange> intFromString(String startS, String endS) {
            int start = (int) readLong(startS);
            int end = (int) readLong(endS);
            return Optional.of(new IntTypedRange(start, end));
        }

        private static Optional<TypedRange> shortFromString(String startS, String endS) {
            short start = (short) readLong(startS);
            short end = (short) readLong(endS);
            return Optional.of(new IntTypedRange(start, end));
        }

        private static Optional<TypedRange> byteFromString(String startS, String endS) {
            byte start = (byte) readLong(startS);
            byte end = (byte) readLong(endS);
            return Optional.of(new IntTypedRange(start, end));
        }

    }

    private static class CharTypedRange extends TypedRange {

        private final String start;
        private final String end;

        private CharTypedRange(char start, char end) {
            super(DataType.CHAR);
            this.start = String.valueOf(start);
            this.end = String.valueOf(end);
        }

        private CharTypedRange(String start, String end) {
            super(DataType.STRING);
            this.start = start;
            this.end = end;
        }

        @Override
        public Pair<Double, Double> getDouble() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Float, Float> getFloat() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Long, Long> getLong() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Integer, Integer> getInt() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Short, Short> getShort() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Byte, Byte> getByte() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Character, Character> getChar() {
            return Pair.with(start.charAt(0), end.charAt(0));
        }

        @Override
        public Pair<Boolean, Boolean> getBoolean() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Object, Object> getObject() {
            return Pair.with(start, end);
        }

        @Override
        public void appendToString(StringBuilder sb) {
            try {
                String startSE = URLEncoder.encode(start, ENC_SCHEME);
                String endSE = URLEncoder.encode(end, ENC_SCHEME);
                sb.append(type.name());
                sb.append(SEP);
                sb.append(startSE);
                sb.append(SEP);
                sb.append(endSE);
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

        private static Optional<TypedRange> charFromString(String startS, String endS) {
            try {
                String startSD = URLDecoder.decode(startS, ENC_SCHEME);
                String endSD = URLDecoder.decode(endS, ENC_SCHEME);
                return Optional.of(new CharTypedRange(startSD.charAt(0), endSD.charAt(0)));
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

        private static Optional<TypedRange> stringFromString(String startS, String endS) {
            try {
                String startSD = URLDecoder.decode(startS, ENC_SCHEME);
                String endSD = URLDecoder.decode(endS, ENC_SCHEME);
                return Optional.of(new CharTypedRange(startSD, endSD));
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

    }

    private static class BoolTypedRange extends TypedRange {

        private final boolean start;
        private final boolean end;

        private BoolTypedRange(boolean start, boolean end) {
            super(DataType.BOOLEAN);
            this.start = start;
            this.end = end;
        }

        @Override
        public Pair<Double, Double> getDouble() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Float, Float> getFloat() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Long, Long> getLong() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Integer, Integer> getInt() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Short, Short> getShort() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Byte, Byte> getByte() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Character, Character> getChar() {
            throw new IllegalArgumentException("DataType is " + type.toString());
        }

        @Override
        public Pair<Boolean, Boolean> getBoolean() {
            return Pair.with(start, end);
        }

        @Override
        public Pair<Object, Object> getObject() {
            return Pair.with(start, end);
        }

        @Override
        public void appendToString(StringBuilder sb) {
            try {
                String startS = Boolean.toString(start);
                String endS = Boolean.toString(end);
                String startSE = URLEncoder.encode(startS, ENC_SCHEME);
                String endSE = URLEncoder.encode(endS, ENC_SCHEME);
                sb.append(type.name());
                sb.append(SEP);
                sb.append(startSE);
                sb.append(SEP);
                sb.append(endSE);
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

        private static Optional<TypedRange> booleanFromString(String startS, String endS) {
            try {
                String startSD = URLDecoder.decode(startS, ENC_SCHEME);
                String endSD = URLDecoder.decode(endS, ENC_SCHEME);
                boolean start = Boolean.valueOf(startSD);
                boolean end = Boolean.valueOf(endSD);
                return Optional.of(new BoolTypedRange(start, end));
            } catch (UnsupportedEncodingException ex) {
                throw new RuntimeException(ex); // if our default encoding isn't supported, that is pretty bad
            }
        }

    }

}

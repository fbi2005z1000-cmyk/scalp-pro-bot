const glossary = {
  RSI: {
    short: 'RSI đo sức mạnh tăng giảm ngắn hạn.',
    detail:
      'RSI thấp thường cho thấy giá đang yếu và có thể bật lên. RSI cao thường cho thấy giá đang nóng và dễ điều chỉnh. Đây là tín hiệu tham khảo, không đảm bảo chắc chắn.',
  },
  MA: {
    short: 'MA là đường trung bình giá.',
    detail:
      'MA7 phản ứng nhanh, MA25 theo xu hướng ngắn hạn, MA99 và MA200 giúp nhận diện xu hướng lớn để tránh vào lệnh ngược chiều.',
  },
  EMA: {
    short: 'EMA là MA nhạy hơn.',
    detail:
      'EMA đặt trọng số lớn hơn cho dữ liệu mới nên bám giá nhanh hơn MA truyền thống, phù hợp quan sát biến động scalp.',
  },
  ATR: {
    short: 'ATR đo độ biến động trung bình của nến.',
    detail:
      'ATR cao nghĩa là biên độ nến lớn, rủi ro rung lắc cao hơn. ATR thấp nghĩa là thị trường ì, dễ thiếu lực cho scalp.',
  },
  ENTRY: {
    short: 'Entry là vùng đề xuất vào lệnh.',
    detail: 'Entry không phải lúc nào cũng vào ngay. Nếu giá lệch vùng vào quá xa, bot sẽ từ chối để tránh đuổi giá.',
  },
  SL: {
    short: 'SL là cắt lỗ bắt buộc.',
    detail: 'SL giúp giới hạn rủi ro mỗi lệnh. Bot sẽ không vào lệnh nếu không xác định được SL hợp lệ.',
  },
  TP: {
    short: 'TP là điểm chốt lời.',
    detail: 'Bot chia TP1/TP2/TP3 để khóa lợi nhuận từng phần và giảm rủi ro khi thị trường đảo chiều.',
  },
  LONG: {
    short: 'LONG là kỳ vọng giá tăng.',
    detail: 'LONG phù hợp khi xu hướng chính tăng, pullback hợp lý và có nến xác nhận rõ ràng.',
  },
  SHORT: {
    short: 'SHORT là kỳ vọng giá giảm.',
    detail: 'SHORT phù hợp khi xu hướng chính giảm, pullback lên vùng cản và có nến xác nhận giảm.',
  },
  SIDEWAY: {
    short: 'Sideway là thị trường đi ngang.',
    detail: 'Vùng sideway có nhiều nhiễu, breakout giả và dễ hit SL. Bot ưu tiên đứng ngoài để bảo toàn vốn.',
  },
  VOLUME: {
    short: 'Volume là khối lượng giao dịch.',
    detail: 'Volume cao giúp xác nhận lực thị trường. Volume thấp thường khiến tín hiệu kém tin cậy.',
  },
  BUY_PRESSURE: {
    short: 'Buy Pressure là lực mua chủ động.',
    detail: 'Điểm cao khi nến tăng thân lớn, đóng gần đỉnh và volume tốt. Dùng để lọc tín hiệu LONG.',
  },
  SELL_PRESSURE: {
    short: 'Sell Pressure là lực bán chủ động.',
    detail: 'Điểm cao khi nến giảm thân lớn, đóng gần đáy và volume tốt. Dùng để lọc tín hiệu SHORT.',
  },
  CONFIDENCE: {
    short: 'Confidence Score là điểm tin cậy 0-100.',
    detail: 'Điểm càng cao thì xác nhận càng đầy đủ. Bot auto chỉ chạy khi điểm đủ cao và qua toàn bộ anti-cháy.',
  },
  RR: {
    short: 'RR là tỷ lệ lời/lỗ kỳ vọng.',
    detail: 'RR 1:2 nghĩa là chấp nhận rủi ro 1 để kỳ vọng lợi nhuận 2. Bot sẽ từ chối lệnh RR quá thấp.',
  },
  PULLBACK: {
    short: 'Pullback là nhịp hồi ngắn.',
    detail: 'Bot ưu tiên vào theo pullback trong xu hướng chính thay vì đuổi nến đã chạy xa.',
  },
  BREAKOUT: {
    short: 'Breakout là phá vỡ vùng cản.',
    detail: 'Breakout cần volume xác nhận. Nếu phá vỡ rồi quay lại ngay thường là fake breakout.',
  },
  FAKE_BREAKOUT: {
    short: 'Fake breakout là phá vỡ giả.',
    detail: 'Đây là bẫy thường gặp khi sideway. Bot hạ điểm hoặc từ chối để tránh vào sai.',
  },
  SESSION: {
    short: 'Session là phiên giao dịch theo giờ UTC.',
    detail:
      'Phiên Europe/US thường có thanh khoản tốt hơn cho scalp. Ngoài phiên chính, bot có thể siết điều kiện để tránh nhiễu.',
  },
  REGIME: {
    short: 'Regime là trạng thái thị trường theo biến động.',
    detail:
      'LOW_VOL: biến động thấp, HIGH_VOL: biến động cao, NORMAL_VOL: phù hợp scalp. Bot dùng regime để tăng hoặc giảm độ khó vào lệnh.',
  },
};

module.exports = glossary;

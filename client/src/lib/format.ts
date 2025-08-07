export const formatNumber = (num: number): string => {
  return num.toLocaleString();
};

export const formatTimestamp = (timestamp: number): string => {
  return new Date(timestamp).toLocaleString();
};

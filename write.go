package promrw

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func Parse(requestBody []byte, samplesHandler func(model.Samples) error) error {

	// decompress the body
	decompressedBody, err := snappy.Decode(nil, requestBody)
	if err != nil {
		return err
	}

	// decode the protobuf
	var promWriteRequest prompb.WriteRequest
	if err := proto.Unmarshal(decompressedBody, &promWriteRequest); err != nil {
		return err
	}

	// convert the protobuf to samples
	samples := promWriteRequestToSamples(&promWriteRequest)

	// call the handler
	return samplesHandler(samples)
}

func promWriteRequestToSamples(promWriteRequest *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range promWriteRequest.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}

	return samples
}
